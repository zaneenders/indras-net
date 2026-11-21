import BTree
import Testing
import _NIOFileSystem

@Test func diskBTreeExample() async throws {
  typealias Accounts = DiskBTree<String, String, Document>

  try await FileSystem.shared.withTemporaryDirectory(
    prefix: nil, options: .init(followSymbolicLinks: false, closeOnExec: true)
  ) { _, path in
    let accounts = try await Accounts.create(in: path)
    let profile: Document = ["name": .string("Zane"), "plan": .string("pro")]
    let originalFile: Document = [
      "name": .string("notes.md"),
      "size": .uint64(4_096),
      "tags": .list([.string("work"), .string("draft")]),
      "caption": .string("Draft notes")
    ]

    try await accounts.withWriteTransaction { transaction in
      try await transaction.updateValue(profile, for: "ACCOUNT#42", sortKey: "PROFILE")
      try await transaction.updateValue(originalFile, for: "ACCOUNT#42", sortKey: "FILE#123")
      try await transaction.updateValue(["name": .string("Mina")], for: "ACCOUNT#43", sortKey: "PROFILE")
    }

    try await accounts.withWriteTransaction { transaction in
      let storedFile = try await transaction.value(for: "ACCOUNT#42", sortKey: "FILE#123")
      var file = try #require(storedFile)
      file["caption"] = nil
      file["verified"] = .bool(true)
      let previousFile = try await transaction.updateValue(file, for: "ACCOUNT#42", sortKey: "FILE#123")
      #expect(previousFile == originalFile)
    }

    let reopened = try await Accounts.open(in: path)
    try await reopened.withReadSnapshot { snapshot in
      let first = try await snapshot.scan(partition: "ACCOUNT#42", after: nil, limit: 1)
      let file = try #require(first.entries.first)
      #expect(file.sortKey == "FILE#123")
      #expect(file.value["caption"] == nil)
      #expect(file.value["verified"] == .bool(true))
      #expect(file.value["size"] == .uint64(4_096))
      #expect(file.value["tags"] == originalFile["tags"])

      let cursor = try #require(first.nextAfter)
      let next = try await snapshot.scan(partition: "ACCOUNT#42", after: cursor, limit: 1)
      #expect(next.entries.map(\.sortKey) == ["PROFILE"])
      #expect(next.entries.map(\.value) == [profile])
      #expect(next.nextAfter == nil)
    }
  }
}
