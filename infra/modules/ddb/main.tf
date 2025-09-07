variable "table_name" { type = string }

resource "aws_dynamodb_table" "url_states" {
  name         = var.table_name
  billing_mode = "PAY_PER_REQUEST"

  hash_key = "url_hash"

  attribute {
    name = "url_hash"
    type = "S"
  }
  attribute {
    name = "domain"
    type = "S"
  }
  attribute {
    name = "last_crawled"
    type = "N"
  }

  global_secondary_index {
    name            = "domain-last-crawled-index"
    hash_key        = "domain"
    range_key       = "last_crawled"
    projection_type = "ALL"
  }

  ttl {
    enabled = false
  }
}

output "table_name" { value = aws_dynamodb_table.url_states.name }
