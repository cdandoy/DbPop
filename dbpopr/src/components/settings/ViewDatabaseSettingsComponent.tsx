import React from "react"
import {DatabaseConfigurationResponse} from "../../api/settingsApi";

export default function ViewDatabaseSettingsComponent({configuration}: {
    configuration: DatabaseConfigurationResponse
}) {
    return <>
        <div>
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