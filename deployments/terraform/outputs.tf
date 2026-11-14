output "postgres_endpoint" {
  description = "RDS PostgreSQL connection endpoint"
  value       = aws_db_instance.postgres.endpoint
}

output "redis_endpoint" {
  description = "ElastiCache Redis primary endpoint"
  value       = "${aws_elasticache_cluster.redis.cache_nodes[0].address}:${aws_elasticache_cluster.redis.cache_nodes[0].port}"
}

output "ecs_cluster_name" {
  description = "ECS Cluster hosting Control Plane and Data Plane services"
  value       = aws_ecs_cluster.this.name
}

