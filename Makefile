.PHONY: up down logs api test fmt lint type
up:
	docker compose up --build -d
down:
	docker compose down -v
logs:
	docker compose logs -f --tail=300
api:
	curl -s http://localhost:8001/health && echo
fmt:
	ruff check --fix .
lint:
	ruff check .
type:
	mypy services libs
test:
	pytest -q
