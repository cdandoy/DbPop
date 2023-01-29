import React, {useEffect, useState} from "react";
import PageHeader from "../pageheader/PageHeader";
import {NavLink} from "react-router-dom";
import {siteApi, SiteResponse} from "../../api/siteApi";

function Section({title, description, to}: {
    title: string;
    description: string | JSX.Element;
    to: string;
}) {
    return <>
        <div className={"mt-5"}>
            <h3>{title}</h3>
            <p>{description}</p>
            <div className={"ms-5 button-bar"}>
                <NavLink to={to}>
                    <button className={"btn btn-primary"}>
                        Next
                        &nbsp;
                        <i className={"fa fa-arrow-right"}/>
                    </button>
                </NavLink>
            </div>
        </div>
    </>
}

export default function DownloadComponent() {
    const [siteResponse, setSiteResponse] = useState<SiteResponse | undefined>();

    useEffect(() => {   // I should use context or redux for that
        siteApi()
            .then(result => {
                setSiteResponse(result.data);
            })
    }, []);

    return <>
        <PageHeader title={"Download"} subtitle={"Download table data to CSV files"}/>

        {siteResponse?.hasSource && (
            <Section title={"Structured Download"}
                     description={"Select multiple tables and the dependencies established based database constraints, and filter the data you want to download."}
                     to={"/download/structured"}/>
        )}

        {siteResponse?.hasSource && (
            <Section title={"Bulk Download"}
                     description={"Select multiple tables and download the full content."}
                     to={"/download/bulk"}/>
        )}

        {siteResponse?.hasTarget && (
            <Section title={"Full Download"}
                     description={<>Dump the full content of your TARGET database back to CSV files.<br/>
                         This might be useful if you use your application to generate additional data and want to save the result to CSV files.</>}
                     to={"/download/target"}/>
        )}
    </>;
}