import React, {useEffect, useState} from "react"
import {DatabaseConfigurationResponse, getSettings} from "../../api/settingsApi";
import PageHeader from "../pageheader/PageHeader";
import ViewDatabaseSettingsComponent from "./ViewDatabaseSettingsComponent";
import {NavLink} from "react-router-dom";

function Database({configuration, type}: { configuration: DatabaseConfigurationResponse, type: string }) {
    return <>
        <div className={"clearfix"}>
            <h3 className={"float-start"}>{type === "source" ? "Source Database" : "Target Database"}</h3>
            <div className={"float-end"}>
                <NavLink to={`/settings/${type}`} className={"btn btn-sm btn-primary"} title={"Edit"}>
                    <i className={"fa fa-edit"}/>
                </NavLink>
            </div>
        </div>
        <div className={"ms-3 mb-5"}>
            {!configuration.url ? <div className={"text-center"}>Disabled</div> : <ViewDatabaseSettingsComponent configuration={configuration}/>}
        </div>
    </>
}

export default function SettingsComponent() {
    const [loading, setLoading] = useState(true);
    const [source, setSource] = useState<DatabaseConfigurationResponse | undefined>()
    const [target, setTarget] = useState<DatabaseConfigurationResponse | undefined>()

    useEffect(() => {
        getSettings()
            .then(result => {
                let settings = result.data;
                setSource(settings.sourceDatabaseConfiguration)
                setTarget(settings.targetDatabaseConfiguration)
                setLoading(false);
            })
    }, [])

    if (loading) return <div><i className={"fa fa-spinner fa-spin"}/> Loading...</div>

    return <>
        <PageHeader title={"Settings"}/>
        <div style={{maxWidth: "50em"}}>
            <Database configuration={source!} type={"source"}/>
            <Database configuration={target!} type={"target"}/>
        </div>
    </>
}