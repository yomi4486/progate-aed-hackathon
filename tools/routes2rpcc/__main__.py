import argparse
import importlib
import importlib.util
import inspect
import os
import sys
from datetime import datetime
from pathlib import Path
from types import ModuleType
from typing import Annotated, Any, Dict, List, Literal, Optional, Union, get_args, get_origin

from fastapi import APIRouter
from pydantic import BaseModel
from starlette.routing import BaseRoute

# Constants for magic numbers
SIMPLE_HTTP_EXCEPTION_MAX_LINES = 2


def type_conv(tp: Any) -> str:
    """Convert Python type to TypeScript type"""
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
        return "unknown"

    if origin is Annotated:
        return type_conv(args[0])

    if inspect.isclass(tp) and issubclass_safe(tp, BaseModel):
        return tp.__name__

    return "unknown"  # Fallback


def issubclass_safe(c: type, cls: type) -> bool:
    try:
        return inspect.isclass(c) and issubclass(c, cls)
    except Exception:
        return False


def extract_router_info(module: ModuleType) -> List[Dict[str, Any]]:
    """Extract APIRouter information from a module"""
    routers: List[Dict[str, Any]] = []

    # Find APIRouter instances in the module
    for name, obj in inspect.getmembers(module):
        if isinstance(obj, APIRouter):
            routers.append({"name": name, "router": obj})

    return routers


def analyze_route(route: BaseRoute) -> Optional[Dict[str, Any]]:
    """Analyze a single FastAPI route"""
    from fastapi.routing import APIRoute

    if not isinstance(route, APIRoute):
        return None

    endpoint_func = route.endpoint

    # Skip routes that only raise HTTPException
    try:
        source = inspect.getsource(endpoint_func)
        if "HTTPException" in source and "raise HTTPException" in source:
            # Check if the function only raises HTTPException
            lines = [line.strip() for line in source.split("\n") if line.strip()]
            func_lines = [
                line
                for line in lines
                if not line.startswith("@") and not line.startswith("def ") and not line.startswith("async def ")
            ]
            if len(func_lines) <= SIMPLE_HTTP_EXCEPTION_MAX_LINES and any("raise HTTPException" in line for line in func_lines):
                return None
    except Exception:
        pass

    # Get function signature
    sig = inspect.signature(endpoint_func)

    # Extract parameter information
    params: List[Dict[str, Any]] = []
    for param_name, param in sig.parameters.items():
        if param_name in ["request", "response"]:  # Skip FastAPI injected params
            continue

        param_type = type_conv(param.annotation) if param.annotation != inspect.Parameter.empty else "unknown"
        is_optional = param.default != inspect.Parameter.empty

        params.append(
            {
                "name": param_name,
                "type": param_type,
                "optional": is_optional,
                "default": param.default if param.default != inspect.Parameter.empty else None,
            }
        )

    # Extract return type
    return_type = type_conv(sig.return_annotation) if sig.return_annotation != inspect.Parameter.empty else "unknown"

    return {
        "path": route.path,
        "methods": list(route.methods),
        "function_name": endpoint_func.__name__,
        "parameters": params,
        "return_type": return_type,
        "endpoint": endpoint_func,
    }


def generate_method_name(path: str, method: str) -> str:
    """Generate TypeScript method name from path and HTTP method"""
    # Remove leading/trailing slashes and split
    path_parts = [part for part in path.strip("/").split("/") if part]

    if not path_parts:
        return method.lower() + "Root"

    # Convert path parts to camelCase
    method_name = path_parts[0].lower()
    for part in path_parts[1:]:
        # Handle path parameters like {id}
        if part.startswith("{") and part.endswith("}"):
            param_name = part[1:-1]
            method_name += param_name.capitalize()
        else:
            method_name += part.capitalize()

    return method_name


def generate_url_construction(path: str, params: List[Dict[str, Any]], method: str) -> str:
    """Generate URL construction code"""
    if method.upper() == "GET":
        # For GET requests, all parameters go to query string
        if not params:
            return f"`${{this.baseUrl}}{path}`"

        query_params: List[str] = []
        for param in params:
            param_name = param["name"]
            if param["optional"]:
                query_params.append(
                    f"{param_name} !== undefined ? '{param_name}=' + encodeURIComponent({param_name}) : ''"
                )
            else:
                query_params.append(f"'{param_name}=' + encodeURIComponent({param_name})")

        # Filter out empty strings and join with '&'
        filtered_params = [f"({param})" for param in query_params]
        query_string = ".filter(Boolean).join('&')"
        
        if len(query_params) == 1:
            return f"`${{this.baseUrl}}{path}?${{{query_params[0]}}}`"
        else:
            params_array = "[" + ", ".join(filtered_params) + "]"
            return f"`${{this.baseUrl}}{path}?${{{params_array}{query_string}}}`"

    else:
        # For POST/PUT/etc, path parameters in URL, body parameters in request body
        return f"`${{this.baseUrl}}{path}`"


def generate_fetch_options(params: List[Dict[str, Any]], method: str) -> str:
    """Generate fetch options for the request"""
    if method.upper() == "GET":
        return ""

    # For non-GET methods, create request body
    if not params:
        return ", { method: '" + method.upper() + "' }"

    body_params = {param["name"]: param["name"] for param in params}
    body_obj = ", ".join(f"{key}" for key in body_params.keys())

    return f", {{ method: '{method.upper()}', headers: {{ 'Content-Type': 'application/json' }}, body: JSON.stringify({{ {body_obj} }}) }}"


def generate_rpc_client(routes_info: List[Optional[Dict[str, Any]]], rpc_prefix: str = "/rpc") -> str:
    """Generate TypeScript RPC client code"""

    # Generate interface methods
    interface_methods: List[str] = []
    implementation_methods: List[str] = []

    for route_info in routes_info:
        if not route_info:
            continue

        path = route_info["path"]
        methods = route_info["methods"]
        params = route_info["parameters"]
        return_type = route_info["return_type"]

        for method in methods:
            method_name = generate_method_name(path, method)

            # Generate method signature
            param_signature: List[str] = []
            for param in params:
                param_name = param["name"]
                param_type = param["type"]
                optional_suffix = "?" if param["optional"] else ""
                param_signature.append(f"{param_name}{optional_suffix}: {param_type}")

            param_str = ", ".join(param_signature)

            # Interface method
            interface_methods.append(f"  {method_name}({param_str}): Promise<{return_type} | ErrorResponse>;")

            # Implementation method
            full_path = rpc_prefix + path
            url_construction = generate_url_construction(full_path, params, method)
            fetch_options = generate_fetch_options(params, method)

            impl_method = f"""  async {method_name}({param_str}): Promise<{return_type} | ErrorResponse> {{
    const response = await fetch({url_construction}{fetch_options});
    if (!response.ok) {{
      return response.json() as Promise<ErrorResponse>;
    }}
    return response.json() as Promise<{return_type}>;
  }}"""
            implementation_methods.append(impl_method)

    # Generate final code
    interface_code = "export interface RPCClient {\n" + "\n".join(interface_methods) + "\n}"

    class_code = f"""export class RPCClientImpl implements RPCClient {{
  private baseUrl: string;

  constructor(baseUrl: string) {{
    this.baseUrl = baseUrl;
  }}

{chr(10).join(implementation_methods)}
}}"""

    return interface_code + "\n\n" + class_code


def derive_types_import_path(output_file: str) -> str:
    """Derive the import path for types based on output file location"""
    output_path = Path(output_file)
    output_dir = output_path.parent
    
    # Look for types directory relative to output file
    types_dir = output_dir / "types"
    if types_dir.exists():
        return "./types"
    
    # Look one level up
    parent_types_dir = output_dir.parent / "types" 
    if parent_types_dir.exists():
        return "../types"
    
    # Default fallback
    return "./types"


def process_router_file(file_path: str, output_file: str) -> None:
    """Process a FastAPI router file and generate TypeScript client"""

    # Get the project root directory (where pyproject.toml exists)
    project_root = Path(__file__).parent.parent.parent.resolve()

    # Add project root to sys.path for imports
    project_root_str = str(project_root)
    if project_root_str not in sys.path:
        sys.path.insert(0, project_root_str)

    # Get relative path from project root
    file_path_obj = Path(file_path).resolve()
    rel_path = file_path_obj.relative_to(project_root)
    module_path = str(rel_path.with_suffix("")).replace(os.sep, ".")
    module_name = module_path

    try:
        # Import the module
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot load module spec from {file_path}")

        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module

        # Set up package for relative imports
        pkg = module_name.rsplit(".", 1)[0] if "." in module_name else module_name
        module.__package__ = pkg

        spec.loader.exec_module(module)

        # Extract router information
        routers_info = extract_router_info(module)

        all_routes: List[Optional[Dict[str, Any]]] = []
        for router_info in routers_info:
            router = router_info["router"]

            # Analyze each route
            for route in router.routes:
                route_info = analyze_route(route)
                all_routes.append(route_info)

        # Generate TypeScript client code
        client_code = generate_rpc_client(all_routes)

        # Derive import path based on output file location
        types_import_path = derive_types_import_path(output_file)
        
        # Add import statement at the top
        imports = f"import type {{ ErrorResponse }} from '{types_import_path}/common';"

        # Collect all unique return types for imports
        return_types: set[str] = set()
        for route in all_routes:
            if route and route["return_type"] != "unknown":
                return_types.add(route["return_type"])

        if return_types:
            type_imports = ", ".join(sorted(return_types))
            imports += f"\nimport type {{ {type_imports} }} from '{types_import_path}/search';"

        final_code = imports + "\n\n" + client_code

        # Write output file
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(final_code + "\n")

        print(f"Generated RPC client: {output_file}")

    finally:
        try:
            sys.path.remove(project_root_str)
        except ValueError:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate TypeScript RPC client from FastAPI router.")
    parser.add_argument(
        "router_file",
        help="Path to FastAPI router file (e.g., app/backend/routers/rpc.py)",
    )
    parser.add_argument(
        "output_file",
        help="Output TypeScript file path (e.g., app/frontend/src/rpc-client.ts)",
    )

    args = parser.parse_args()

    router_file = os.path.abspath(args.router_file)
    output_file = os.path.abspath(args.output_file)

    if not os.path.exists(router_file):
        print(f"Error: Router file not found: {router_file}")
        sys.exit(1)

    process_router_file(router_file, output_file)
    print("RPC client generation completed!")


if __name__ == "__main__":
    main()
