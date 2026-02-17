/// WasmEdge SDK v0.14 spike test.
///
/// Verifies:
/// 1. wasmedge-sdk compiles and links
/// 2. Can load a .wasm module from WAT
/// 3. Can register a host function and have the guest call it
/// 4. Can access guest linear memory from a host function
use std::collections::HashMap;

use wasmedge_sdk::{
    params,
    vm::SyncInst,
    CallingFrame, ImportObjectBuilder, Module, Store, Vm, WasmVal, WasmValue,
};
use wasmedge_sys::Instance as SysInstance;
use wasmedge_types::error::CoreError;

// Host state shared with host functions
struct SpikeHostState {
    captured_value: i32,
}

// Host function: captures a value from the guest
fn host_capture(
    data: &mut SpikeHostState,
    _inst: &mut SysInstance,
    _frame: &mut CallingFrame,
    args: Vec<WasmValue>,
) -> Result<Vec<WasmValue>, CoreError> {
    data.captured_value = args[0].to_i32();
    Ok(vec![WasmValue::from_i32(0)])
}

// Host function: reads from guest memory
fn host_read_memory(
    _data: &mut SpikeHostState,
    _inst: &mut SysInstance,
    frame: &mut CallingFrame,
    args: Vec<WasmValue>,
) -> Result<Vec<WasmValue>, CoreError> {
    let ptr = args[0].to_i32() as u32;
    let len = args[1].to_i32() as u32;

    let memory = frame.memory_ref(0).expect("no memory");
    let bytes = memory
        .get_data(ptr, len)
        .expect("get_data failed");

    // Sum the bytes as a simple check
    let sum: i32 = bytes.iter().map(|&b| b as i32).sum();
    Ok(vec![WasmValue::from_i32(sum)])
}

#[test]
fn test_load_and_call_simple_wasm() {
    // Simple WAT: export a function that adds two numbers
    let wat = br#"
    (module
        (func $add (param i32 i32) (result i32)
            local.get 0
            local.get 1
            i32.add
        )
        (export "add" (func $add))
    )
    "#;

    let wasm_bytes = wasmedge_sdk::wat2wasm(wat).expect("wat2wasm failed");
    let module = Module::from_bytes(None, wasm_bytes).expect("Module::from_bytes failed");

    let mut vm = Vm::new(Store::new(None, HashMap::<String, &mut dyn SyncInst>::new()).unwrap());
    vm.register_module(None, module)
        .expect("register_module failed");

    let result = vm.run_func(None, "add", params!(3i32, 4i32)).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].to_i32(), 7);
}

#[test]
fn test_host_function_registration() {
    // WAT that imports and calls a host function
    let wat = br#"
    (module
        (import "test" "capture" (func $capture (param i32) (result i32)))
        (func $call_capture (export "call_capture") (param i32) (result i32)
            local.get 0
            call $capture
        )
    )
    "#;

    let wasm_bytes = wasmedge_sdk::wat2wasm(wat).expect("wat2wasm failed");
    let module = Module::from_bytes(None, wasm_bytes).expect("Module::from_bytes failed");

    let state = SpikeHostState { captured_value: 0 };
    let mut import_builder = ImportObjectBuilder::new("test", state).unwrap();
    import_builder
        .with_func::<i32, i32>("capture", host_capture)
        .unwrap();
    let mut import = import_builder.build();

    let mut instances: HashMap<String, &mut dyn SyncInst> = HashMap::new();
    instances.insert("test".to_string(), &mut import);

    let mut vm = Vm::new(Store::new(None, instances).unwrap());
    vm.register_module(None, module)
        .expect("register_module failed");

    let result = vm
        .run_func(None, "call_capture", params!(42i32))
        .unwrap();
    assert_eq!(result[0].to_i32(), 0); // host_capture returns 0
}

#[test]
fn test_host_function_reads_guest_memory() {
    // WAT with memory that stores known bytes and calls host to read them
    let wat = br#"
    (module
        (import "test" "read_mem" (func $read_mem (param i32 i32) (result i32)))
        (memory (export "memory") 1)
        (data (i32.const 0) "\01\02\03\04\05")
        (func $test_read (export "test_read") (result i32)
            ;; Call host function to read 5 bytes starting at offset 0
            i32.const 0
            i32.const 5
            call $read_mem
        )
    )
    "#;

    let wasm_bytes = wasmedge_sdk::wat2wasm(wat).expect("wat2wasm failed");
    let module = Module::from_bytes(None, wasm_bytes).expect("Module::from_bytes failed");

    let state = SpikeHostState { captured_value: 0 };
    let mut import_builder = ImportObjectBuilder::new("test", state).unwrap();
    import_builder
        .with_func::<(i32, i32), i32>("read_mem", host_read_memory)
        .unwrap();
    let mut import = import_builder.build();

    let mut instances: HashMap<String, &mut dyn SyncInst> = HashMap::new();
    instances.insert("test".to_string(), &mut import);

    let mut vm = Vm::new(Store::new(None, instances).unwrap());
    vm.register_module(None, module)
        .expect("register_module failed");

    let result = vm.run_func(None, "test_read", params!()).unwrap();
    // Sum of bytes [1,2,3,4,5] = 15
    assert_eq!(result[0].to_i32(), 15);
}
