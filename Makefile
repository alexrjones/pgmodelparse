BUILD_DIR = .
BIN = pgmodelgen

.PHONY: build clean

build:
	@mkdir -p $(BUILD_DIR)
	@go build -o $(BUILD_DIR)/$(BIN)

clean:
	@rm $(BUILD_DIR)/$(BIN)
