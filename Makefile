# F1 Data Pipeline Development Makefile

.PHONY: dev-build dev-start dev-stop dev-down dev-logs dev-restart clean help

# Configuration
CONFIG_FILE = config/dev.yaml
ENVIRONMENT = development

# Development commands
dev-build:
	@echo "🔨 Building Docker images for development..."
	@echo "📋 Using configuration: $(CONFIG_FILE)"
	@echo "🌍 Environment: $(ENVIRONMENT)"
	docker-compose build
	@echo "✅ Build completed successfully"

dev-start:
	@echo "🚀 Starting F1 Data Pipeline development environment..."
	@echo "📋 Using configuration: $(CONFIG_FILE)"
	@echo "🌍 Environment: $(ENVIRONMENT)"
	@echo "🔧 Setting environment variables..."
	@export F1_ENVIRONMENT=$(ENVIRONMENT) && \
	export F1_CONFIG_PATH=$(CONFIG_FILE) && \
	docker-compose up -d
	@echo ""
	@echo "🎉 Services started successfully!"
	@echo "📊 Airflow UI: http://localhost:8080 (admin/admin)"
	@echo "⚡ Spark UI: http://localhost:8081"
	@echo "📝 View logs with: make dev-logs"

dev-stop:
	@echo "⏹️  Stopping F1 Data Pipeline services..."
	docker-compose stop
	@echo "✅ Services stopped"

dev-down:
	@echo "🛑 Shutting down F1 Data Pipeline services..."
	docker-compose down
	@echo "✅ Services shut down"

dev-logs:
	@echo "📋 Showing logs for F1 Data Pipeline services..."
	@echo "📊 Configuration: $(CONFIG_FILE)"
	docker-compose logs -f

dev-restart:
	@echo "🔄 Restarting F1 Data Pipeline services..."
	@echo "📋 Using configuration: $(CONFIG_FILE)"
	docker-compose down
	@export F1_ENVIRONMENT=$(ENVIRONMENT) && \
	export F1_CONFIG_PATH=$(CONFIG_FILE) && \
	docker-compose up -d
	@echo "✅ Services restarted successfully"

# Cleanup commands
clean:
	@echo "🧹 Cleaning up Docker resources..."
	docker-compose down -v
	docker system prune -f
	@echo "✅ Cleanup completed"

# Test commands
test-iceberg:
	@echo "🧪 Testing AWS Glue Data Catalog and Iceberg integration..."
	@echo "📋 Using configuration: $(CONFIG_FILE)"
	@echo "🌍 Environment: $(ENVIRONMENT)"
	@if [ ! -f .env ]; then \
		echo "❌ .env file not found. Please copy .env.example to .env and configure it."; \
		exit 1; \
	fi
	@echo "🐳 Running test in Spark master container..."
	docker-compose exec spark-master python /opt/spark/scripts/test_iceberg_glue_connection.py

# Help command
help:
	@echo "F1 Data Pipeline Development Commands:"
	@echo ""
	@echo "🔧 Development Commands:"
	@echo "  dev-build        Build Docker images for development"
	@echo "  dev-start        Start all services with dev.yaml config"
	@echo "  dev-stop         Stop all services (keep containers)"
	@echo "  dev-down         Shut down all services and containers"
	@echo "  dev-logs         Show logs from all services"
	@echo "  dev-restart      Restart all services with dev.yaml config"
	@echo ""
	@echo "🧪 Testing Commands:"
	@echo "  test-iceberg     Test AWS Glue Data Catalog and Iceberg integration"
	@echo ""
	@echo "🧹 Cleanup Commands:"
	@echo "  clean           Stop services and clean Docker resources"
	@echo ""
	@echo "❓ Help:"
	@echo "  help            Show this help message"
	@echo ""
	@echo "📋 Configuration:"
	@echo "  Config File:    $(CONFIG_FILE)"
	@echo "  Environment:    $(ENVIRONMENT)"