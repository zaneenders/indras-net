# Repository rules

- No default arguments in repository code, including tests. Keep every argument
  explicit at call sites; do not hide defaults behind overloads or wrappers.
- Keep documentation to commands and at most one short line explaining each command.
  Leave prose documentation to the user unless explicitly asked to write it.
