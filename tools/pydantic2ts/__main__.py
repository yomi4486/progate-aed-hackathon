import argparse
import ast
import importlib
import importlib.util
import inspect
import os
import sys
from datetime import datetime
from types import ModuleType
from typing import Annotated, Any, Dict, List, Literal, Union, get_args, get_origin

from pydantic import BaseModel, HttpUrl


def type_conv(tp: Any) -> str:
    origin = get_origin(tp)
    args = get_args(tp)

    if origin is Union:
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return type_conv(non_none[0])
        return " | ".join(sorted({type_conv(a) for a in non_none}))

    if origin in (list, List):
        return f"Array<{type_conv(args[0])}>"

    if origin is Literal:
        lits: list[str] = []
        for a in args:
            if isinstance(a, str):
                lits.append(f'"{a}"')
            else:
                lits.append(str(a))
        return " | ".join(lits)

    if origin in (dict, Dict):
        k, v = args or (str, Any)
        return f"Record<{type_conv(k)}, {type_conv(v)}>"

    if tp in (int, float):
        return "number"
    if tp is bool:
        return "boolean"
    if tp is str:
        return "string"
    if tp is datetime:
        return "string"  # ISO 8601 format

    if tp is Any or tp is object:
        return "any"

    if origin is Annotated:
        return type_conv(args[0])

    if inspect.isclass(tp) and issubclass_safe(tp, BaseModel):
        return tp.__name__

    if inspect.isclass(tp) and issubclass_safe(tp, HttpUrl):
        return "string"

    return "any"  # Fallback


def model_conv(cls: type[BaseModel]) -> str:
    lines = [f"export interface {cls.__name__} {{"]
    for name, field in cls.model_fields.items():
        ts_type = type_conv(field.annotation)
        optional = "?" if not field.is_required() else ""
        lines.append(f"  {name}{optional}: {ts_type};")
    lines.append("}")
    return "\n".join(lines)


def issubclass_safe(c: type, cls: type) -> bool:
    try:
        return inspect.isclass(c) and issubclass(c, cls)
    except Exception:
        return False


def collect_models(module: ModuleType) -> list[type[BaseModel]]:
    models: list[type[BaseModel]] = []
    for _, obj in inspect.getmembers(module, inspect.isclass):
        if issubclass_safe(obj, BaseModel) and obj.__module__ == module.__name__:
            models.append(obj)
    return sorted(models, key=lambda c: c.__name__)


def process_file(file_path: str, output_dir: str, base_dir: str) -> None:
    package_name = os.path.basename(base_dir)
    rel = os.path.splitext(os.path.relpath(file_path, base_dir))[0]
    module_rel = rel.replace(os.sep, ".")
    module_name = package_name if module_rel in ("", "__init__") else f"{package_name}.{module_rel}"

    # インポート元を把握する
    defined_literal_aliases: set[str] = set()
    imported_from: dict[str, str] = {}
    try:
        with open(file_path, "r", encoding="utf-8") as rf:
            src = rf.read()
        tree = ast.parse(src, filename=file_path)
        for node in ast.walk(tree):
            # ローカル定義
            if isinstance(node, ast.Assign):
                if isinstance(node.value, ast.Subscript):
                    target = node.value.value
                    is_literal = (isinstance(target, ast.Name) and target.id == "Literal") or (
                        isinstance(target, ast.Attribute) and target.attr == "Literal"
                    )
                    if is_literal:
                        for t in node.targets:
                            if isinstance(t, ast.Name):
                                defined_literal_aliases.add(t.id)
            # import 元の特定
            if isinstance(node, ast.ImportFrom) and node.names:
                base = (node.module or "").split(".")[-1] if node.module else ""
                for alias in node.names:
                    local_name = alias.asname or alias.name
                    if local_name:
                        imported_from[local_name] = base or imported_from.get(local_name, "")
    except Exception:
        pass

    parent_dir = os.path.dirname(base_dir)
    sys.path.insert(0, parent_dir)
    try:
        # 親パッケージを事前インポート
        importlib.import_module(package_name)

        spec = importlib.util.spec_from_file_location(module_name, file_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot load module spec from {file_path}")

        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        # 相対インポート用
        pkg = module_name.rsplit(".", 1)[0] if "." in module_name else module_name
        module.__package__ = pkg

        spec.loader.exec_module(module)
    finally:
        try:
            sys.path.remove(parent_dir)
        except ValueError:
            pass

    models = collect_models(module)
    output: List[str] = []
    import_lines: List[str] = []

    # 参照追跡セット
    local_model_names = {m.__name__ for m in models}
    referenced_aliases: set[str] = set()
    referenced_models: set[str] = set()

    # モジュールのシンボル辞書
    module_vars = vars(module)

    def conv(tp: Any) -> str:
        origin = get_origin(tp)
        args = get_args(tp)

        if origin is Union:
            non_none = [a for a in args if a is not type(None)]
            if len(non_none) == 1:
                return conv(non_none[0])
            return " | ".join(sorted({conv(a) for a in non_none}))

        if origin in (list, List):
            return f"Array<{conv(args[0])}>"

        if origin is Literal:
            alias_name = None
            for name, val in module_vars.items():
                if val is tp:
                    alias_name = name
                    break
            if alias_name:
                # ローカル未定義 -> import 対象として記録
                if alias_name not in defined_literal_aliases:
                    referenced_aliases.add(alias_name)
                return alias_name
            # 名前が取れない場合 union を展開
            lits: list[str] = []
            for a in args:
                lits.append(f'"{a}"' if isinstance(a, str) else str(a))
            return " | ".join(lits)

        if origin in (dict, Dict):
            k, v = args or (str, Any)
            return f"Record<{conv(k)}, {conv(v)}>"

        if tp in (int, float):
            return "number"
        if tp is bool:
            return "boolean"
        if tp is str:
            return "string"
        if tp is datetime:
            return "string"
        if tp is Any or tp is object:
            return "any"
        if origin is Annotated:
            return conv(args[0])

        if inspect.isclass(tp) and issubclass_safe(tp, BaseModel):
            name = tp.__name__
            if name not in local_model_names:
                referenced_models.add(name)
            return name

        if inspect.isclass(tp) and issubclass_safe(tp, HttpUrl):
            return "string"

        return "any"

    for name, obj in module_vars.items():
        if name in defined_literal_aliases and get_origin(obj) is Literal:
            lits = " | ".join([f'"{a}"' if isinstance(a, str) else str(a) for a in get_args(obj)])
            output.append(f"export type {name} = {lits};")

    # 参照追跡とモデルの出力
    for cls in models:
        lines = [f"export interface {cls.__name__} {{"]
        for name, field in cls.model_fields.items():
            ts_type = conv(field.annotation)
            optional = "?" if not field.is_required() else ""
            lines.append(f"  {name}{optional}: {ts_type};")
        lines.append("}")
        output.append("\n".join(lines))

    to_import: dict[str, list[str]] = {}
    for name in sorted(referenced_aliases | referenced_models):
        if name in defined_literal_aliases or name in local_model_names:
            continue
        src_base = imported_from.get(name)
        if not src_base:
            continue
        to_import.setdefault(src_base, []).append(name)

    for base, names in sorted(to_import.items()):
        spec = ", ".join(sorted(set(names)))
        import_lines.append(f"import type {{ {spec} }} from './{base}';")

    if output:
        os.makedirs(output_dir, exist_ok=True)
        out_name = module_name.split(".")[-1]
        output_file = os.path.join(output_dir, f"{out_name}.ts")
        with open(output_file, "w", encoding="utf-8") as f:
            if import_lines:
                f.write("\n".join(import_lines) + "\n\n")
            f.write("\n\n".join(output) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate TypeScript definitions from Pydantic models.")
    parser.add_argument(
        "input_dir",
        help="Directory containing Python files with Pydantic models.",
    )
    parser.add_argument(
        "output_dir",
        help="Directory to output TypeScript definition files.",
    )
    args = parser.parse_args()

    input_dir = os.path.abspath(args.input_dir)
    output_dir = os.path.abspath(args.output_dir)

    for root, _, files in os.walk(input_dir):
        for file in files:
            if file.endswith(".py") and not file.startswith("_"):
                file_path: str = os.path.join(root, file)  # type: ignore[var-annotated]
                if isinstance(file_path, str):
                    process_file(file_path, output_dir, input_dir)

    print("success!")
    print(f"from {input_dir}/*.py -> {output_dir}/*.ts")


if __name__ == "__main__":
    main()
