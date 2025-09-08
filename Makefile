.PHONY: run tf-init tf-apply tf-destroy

# 🌟 LocalStackを華麗に起動！AWSをローカルで再現しよう！🦄🌈
run:
	@echo "🌟 LocalStackを起動中... AWSの魔法をローカルで体験！🪄🐳"
	docker-compose -f localstack/docker-compose.yml up -d # 🐳 LocalStackコンテナをバックグラウンドで起動！

tf-init:
	cd infra && terraform init -backend=false

tf-apply:
	cd infra && terraform apply -auto-approve -var-file=devlocal.tfvars

tf-destroy:
	cd infra && terraform destroy -auto-approve -var-file=devlocal.tfvars