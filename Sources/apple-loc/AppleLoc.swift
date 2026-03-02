import ArgumentParser

@main
struct AppleLoc: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "apple-loc",
        abstract: "Search Apple's official localization data from the terminal.",
        subcommands: [IngestCommand.self, EmbedCommand.self, SearchCommand.self, LookupCommand.self, InfoCommand.self, SelftestCommand.self]
    )
}
