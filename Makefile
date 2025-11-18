PROTO_FILES := proto/messages.proto
PY_PROTO_OUT := python/proto

.PHONY: all build-proto clean lint

all: build-proto

build-proto:
	@mkdir -p $(PY_PROTO_OUT)
	uv run --project python python -m grpc_tools.protoc \
		--proto_path=proto \
		--python_out=$(PY_PROTO_OUT) \
		--grpc_python_out=$(PY_PROTO_OUT) \
		$(PROTO_FILES)
	@python scripts/fix_proto_imports.py

clean:
	rm -rf target
	rm -rf $(PY_PROTO_OUT)

lint:
	uv run --project python ruff format python
	uv run --project python ruff check python --fix
	uv run --project python ty check python --exclude python/proto/messages_pb2_grpc.py
	cargo fmt
	cargo clippy --all-targets --all-features -- -D warnings
