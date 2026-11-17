.PHONY: build test check client-check ui-check ui-build release clean

NAME := print-queue
VERSION := 0.3.0
PLATFORMS := linux_amd64 linux_arm64 darwin_amd64 darwin_arm64 windows_amd64

build:
	go build -trimpath -o bin/$(NAME) .

test:
	go test ./...

client-check:
	cd client && pnpm install --frozen-lockfile && pnpm run build

ui-check: client-check
	cd ui && pnpm install --frozen-lockfile && pnpm run check:svelte && pnpm run build

check: test client-check ui-check build

release: ui-check
	rm -rf dist
	mkdir -p dist
	cp -R ui/dist/. dist/
	for platform in $(PLATFORMS); do \
		GOOS=$${platform%_*}; GOARCH=$${platform#*_}; \
		ext=; if [ "$$GOOS" = windows ]; then ext=.exe; fi; \
		GOOS=$$GOOS GOARCH=$$GOARCH CGO_ENABLED=0 go build -trimpath -o "dist/$(NAME)_$${platform}$$ext" .; \
	done
	chmod -R a+rX dist
	sh scripts/write-checksums.sh dist
	rm -rf release
	mkdir -p release
	tar -czf release/$(NAME)_$(VERSION).tar.gz -C dist module.yaml checksums.txt $$(find dist -maxdepth 1 -type f ! -name module.yaml ! -name checksums.txt -printf '%f\n' | LC_ALL=C sort) ui

clean:
	rm -rf bin dist release client/dist ui/dist
