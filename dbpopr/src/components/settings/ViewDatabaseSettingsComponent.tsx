import React from "react"
import {DatabaseConfiguration} from "../../api/settingsApi";

export default function ViewDatabaseSettingsComponent({configuration}: {
    configuration: DatabaseConfiguration
}) {
    return <>
        <div>
            {configuration.conflict && <>
                <div className="alert alert-danger" role="alert">
                    There is a conflict between environment variables and the configuration file.
                </div>
            </>}
            <div className="form-group">
                <label htmlFor="url">URL</label>
                <span className="form-control" id="url" style={{overflowX: "hidden"}}>
                    {configuration.url}
                </span>
            </div>
            <div className="form-group">
                <label htmlFor="username">Username</label>
                <span className="form-control" id="username" style={{overflowX: "hidden"}}>
                    {configuration.username}
                </span>
            </div>
        </div>
    </>
}