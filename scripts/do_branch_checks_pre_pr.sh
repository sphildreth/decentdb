#!/bin/bash

nimble build_lib && nimble build && nimble test && nimble test_bindings

./examples/run_all.py

