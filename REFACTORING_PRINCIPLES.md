# Refactoring Principles for Rust

## 1. The "Fearless Refactoring" Loop

In Rust, the compiler is your strongest safety net. If it compiles after a structural change, there is a remarkably high chance that the behavior remains intact.

### Micro-Refactorings

Refactor in the smallest possible steps to leverage compiler feedback immediately:

- Extract one expression into a variable.
- Rename a struct or function.
- Change a single function argument.
- Extract a tiny helper function or closure.
- Replace `&mut self` with `&self` + interior mutability (e.g., `Cell` or `RefCell`) when architectural immutability is required but local mutation is needed.

**Compile -> Commit -> Repeat.**

## 2. Structural & Type-Level Principles

### Make Invalid States Unrepresentable

This is arguably the strongest Rust principle. Often, the best refactoring is simply changing the data type itself to eliminate edge cases at compile-time.

- **Empty is invalid:** Change `Option<String>` to `String`.
- **Implicit relationships:** Change `(f64, f64)` to `struct Point { x: f64, y: f64 }`.
- **Complex collections:** Change `Vec<(String, u32)>` to a `HashMap<String, Entry>` or a custom struct using the Newtype pattern.
- **Mutually exclusive flags:** Change multiple `bool` fields into a single sum type (`enum`).

### Single Responsibility at Rust Scale

- **Functions:** Should do one semantic thing.
- **Modules:** Should have one clear domain boundary.
- **Traits:** Should have one coherent purpose (prefer small, focused traits like `Read` or `Write` over massive, multi-method interfaces).

### Ownership & Borrowing Progression

When untangling messy borrowing, try to move code from the "easy/expensive" side to the "hard/efficient" side iteratively:

- `Clone` everywhere + `String` + `Vec` -> `Cow` / borrow where possible
- `&mut` everywhere -> `&self` + `Cell` / `RefCell` / `Mutex`
- Many `.clone()` calls on big data -> `Arc<T>` / `Rc<T>` (when single-threaded)
- `Box<dyn Trait>` (dynamic dispatch) -> Generic `<T: Trait>` (static dispatch)

### Composition & Generics

- Prefer traits and generics over trait objects (`dyn Trait`) when possible to avoid runtime overhead.
- Prefer free functions over methods when it improves ergonomics or avoids unnecessary borrowing of `self`.
- Lean heavily on iterator combinators (`map`, `filter`, `flat_map`, `collect`).

## 3. Narrative Flow and Readability

Make the code easy to read top-to-bottom. Avoid the "arrow anti-pattern" (deeply nested code).

- **Fail Fast:** Use early returns for error or precondition cases.
- **Embrace `?`:** Use the `?` operator aggressively to propagate errors.
- **Flatten Scope:** Avoid deep nesting by extracting functions or using `Option`/`Result` combinators like `and_then` and `map`.
- **Be Explicit:** Name variables and arguments clearly; avoid cryptic abbreviations.

### Error Handling

Error handling should actively improve during a refactor, not just change shape.

- **Bad:** `(T, bool)` or `Option<T>` for fallible operations.
- **Better:** `Result<T, E>`.
- **Even Better:** Domain-specific error enums.
- **Best:** Using `anyhow::Result` / `eyre` for applications, or `thiserror` for libraries to provide rich context.

## 4. Architecture and Tooling

### Module Structure

- **Avoid God-Modules:** Break up `lib.rs` or `main.rs` files that exceed 1500-2000 lines of code.
- **Group by Domain:** Organize files by business domain (e.g., `user.rs`, `billing.rs`), not by technical type (`structs.rs`, `errors.rs` are considered harmful).
- **Encapsulate:** Use `pub(crate)` generously to hide internal implementation details from the public API.
- **Extract Crates:** Consider Cargo Workspaces and splitting logic into multiple crates earlier than you think. It drastically improves compile times and enforces strict dependency boundaries.

### Clippy is Your Co-Pilot

Many refactorings are quite literally just "doing what Clippy suggests." Enable almost everything in your workspace `Cargo.toml`:

```toml
[workspace.lints.clippy]
all = "warn"
pedantic = "warn"
nursery = "allow"   # or warn selectively
cargo = "warn"
```

## 5. Performance Last

- **Make it Correct:** First make the code correct, clear, and well-factored.
- **Make it Fast:** Only micro-optimize allocations, cache locality, and threading after the structure is clean.
- **Measure First:** Never guess. Use `criterion.rs` for benchmarking and generate flamegraphs before and after performance-related refactors.

## 6. The Polish: Naming, Layout, and Documentation

This is where code transforms from merely "compiling" to "production-grade." When tackling a massive codebase, standardizing these elements drastically reduces the cognitive load for anyone reading the code.

### Sound and Semantic Naming

Rust has strict naming conventions (`snake_case` for variables/functions, `PascalCase` for types), but semantic naming requires active thought.

- **Express Intent, Not Implementation:** Name variables for what they represent in the business domain, not their data type (e.g., `active_users` instead of `user_vec`).
- **Avoid Stuttering:** Because Rust relies heavily on modules and namespaces, avoid repeating the module or struct name in its methods.
  - Bad: `user::User::new_user()`, `server.start_server()`
  - Good: `user::User::new()`, `server.start()`
- **Boolean Naming:** Prefix booleans with verbs like `is_`, `has_`, or `can_` (e.g., `is_active`, `has_permission`).

### Ruthless DRY (Don't Repeat Yourself)

In older or quickly written codebases, it is common to find multiple functions doing the exact same thing with slight variations.

- **Identify Structural Duplication:** Look for repeated loops, identical API calls, or duplicated error-handling boilerplate.
- **Leverage Generics and Traits:** If you have `process_user_data` and `process_admin_data` doing the same thing, refactor them into a single generic function `fn process_data<T: Role>(data: T)`.
- **Use Macros (Sparingly):** If you have extreme boilerplate that cannot be abstracted via generics or functions, declarative macros (`macro_rules!`) can be used to DRY up the code, though they should be a last resort to preserve readability.

### Logical Layout and Sorting

A well-structured Rust file should read like a standardized document. Enforce a consistent top-to-bottom layout in every file:

1. **Lint Attributes:** `#![warn(...)]` or `#![allow(...)]` at the very top.
2. **Module Declarations:** `mod sub_module;`
3. **Imports:** `use crate::...;` (Alphabetized and grouped).
4. **Constants / Statics:** Module-level immutable state.
5. **Primary Data Types:** `pub struct` or `pub enum`.
6. **Primary Implementations:** `impl MyStruct { ... }` (Constructors first, then public methods, then private helpers).
7. **Trait Implementations:** `impl MyTrait for MyStruct { ... }`.
8. **Standalone/Helper Functions:** Private `fn`s used within the module.
9. **Tests:** `#[cfg(test)] mod tests { ... }` always at the very bottom.

### Robustness Beyond the Compiler

The borrow checker ensures memory safety, but logical robustness requires defensive programming.

- **Exhaustive Matching:** Avoid using the catch-all `_ => {}` in `match` statements for enums unless absolutely necessary. If someone adds a new variant to that enum later, you want the compiler to break and force them to handle it explicitly.
- **Type-State Pattern for Workflows:** If a process has distinct stages (e.g., Draft -> Pending -> Published), create separate structs for each state. This makes it impossible to accidentally publish a draft, as the `publish()` method only exists on the `Pending` struct.

### Beautiful, Commented Code

Documentation is not an afterthought; it is part of the API.

- **Enable `#![warn(missing_docs)]`:** Force yourself to document all public-facing elements.
- **Know the Difference Between `//` and `///`:**
  - Use `///` (Rustdoc) to explain the *What* and *How* for consumers of the API.
  - Use `//` (Inline comments) inside the function to explain the *Why* behind a complex block of logic.
- **Standardized Doc Sections:** Every complex public function should have these sections in its Rustdoc:
  - `# Examples`: A doctest that actually compiles and runs.
  - `# Errors`: Explicitly state under what conditions this function will return an `Err`.
  - `# Panics`: If the function contains `unwrap()`, `expect()`, or array indexing that could panic, document exactly when that would happen.
