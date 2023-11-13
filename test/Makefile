process:
	@node -r dotenv/config lib/main.js

build:
	@npm run build

typegen:
	@npx squid-substrate-typegen typegen.json

.PHONY: build serve process migrate codegen typegen up down
