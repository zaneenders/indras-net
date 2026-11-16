# Repository rules

- No default arguments in repository code, including tests. Keep every argument
  explicit at call sites; do not hide defaults behind overloads or wrappers.
- Prefer modern Swift APIs and language features over legacy `NS`-prefixed APIs
  and attributes. For synchronization, prefer `import Synchronization` and its
  primitives, such as `Mutex`, over `NSLock`.
- Never use `@unchecked Sendable`; use compiler-checked `Sendable` conformance.
- Keep documentation to commands and at most one short line explaining each command.
  Leave prose documentation to the user unless explicitly asked to write it.
- `swift-subprocess`, `swift-collections`, `swift-nio`, `swift-configuration`,
  `swift-argument-parser`, and `swift-algorithms` are free to use when needed.
- Swift compiler source is available at `~/Developer/swift-project` when needed.
