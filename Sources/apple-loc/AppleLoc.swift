import ArgumentParser

@main
struct AppleLoc: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "apple-loc",
        abstract: "Search Apple's official localization data from the terminal.",
        subcommands: [IngestCommand.self, SearchCommand.self, LookupCommand.self, SelftestCommand.self]
    )
}
