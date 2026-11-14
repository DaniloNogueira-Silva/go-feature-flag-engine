variable "aws_region" {
  description = "AWS region to deploy the Feature Flag Engine infrastructure"
  type        = string
  default     = "eu-central-1"
}

variable "project_name" {
  description = "Project identifier prefix"
  type        = string
  default     = "feature-flag-engine"
}

variable "environment" {
  description = "Deployment environment (staging or production)"
  type        = string
  default     = "production"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.42.0.0/16"
}

variable "db_instance_class" {
  description = "RDS PostgreSQL instance type"
  type        = string
  default     = "db.t4g.micro"
}

variable "db_username" {
  description = "PostgreSQL master username"
  type        = string
  default     = "ff_admin"
}

variable "db_password" {
  description = "PostgreSQL master password"
  type        = string
  sensitive   = true
}

variable "redis_node_type" {
  description = "ElastiCache Redis node instance size"
  type        = string
  default     = "cache.t4g.micro"
}

