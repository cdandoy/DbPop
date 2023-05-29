import React from "react"

export default function FlywayCreated({flywayGeneratedFilename, snapshotFilename}: { flywayGeneratedFilename: string, snapshotFilename: string }) {
    return <div>
        <h5>Flyway Script Created</h5>
        <p>A new Flyway script has been created: <code>{flywayGeneratedFilename}</code></p>
        <p>And a new snapshot has been captured: <code>{snapshotFilename}</code></p>
    </div>
}