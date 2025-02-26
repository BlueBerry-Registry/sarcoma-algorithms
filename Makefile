PUSH ?= false

help:
	@echo "Usage:"
	@echo "  make help      - show this message"
	@echo "  make session   - build the session image"
	@echo "  make analytics - build the analytics image"
	@echo ""
	@echo "Using "
	@echo "  PUSH: ${PUSH} (true/false)"
	@echo ""

session:
	cd v6-sessions && make image PUSH_REG=$(PUSH)

analytics:
	cd v6-analytics && make image PUSH_REG=$(PUSH)