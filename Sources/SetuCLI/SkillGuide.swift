enum SkillGuide {
  static let marker = "<!-- Installed by setu setup skill. -->"

  static let stub = """
    ---
    name: setu
    description: Install and configure Setu nodes, transfer files between peers, and manage Setu transfer jobs.
    metadata:
      setu-stub: "1"
    ---
    \(marker)

    Run `setu agent` and follow the instructions it prints.
    """ + "\n"
}
