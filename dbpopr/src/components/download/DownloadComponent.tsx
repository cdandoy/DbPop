import React, {useEffect, useState} from "react";
import PageHeader from "../pageheader/PageHeader";
import {NavLink} from "react-router-dom";
import {siteApi, SiteResponse} from "../../api/siteApi";
import './DownloadComponent.scss'
import structured_download from "./structured_download.png"
import bulk_download from "./bulk_download.png"
import full_download from "./full_download.png"

function Section({title, description, to, img}: {
    title: string;
    description: string | JSX.Element;
    to: string;
    img?: any;
}) {
    return <>
        <div className={"download-component-section row mt-5"}>
            <div className={"col-8"}>
                <h3>{title}</h3>
                <p>{description}</p>
                <div className={"ms-5 button-bar"}>
                    <NavLink to={to}>
                        <button className={"btn btn-primary"}>
                            Select
                        </button>
                    </NavLink>
                </div>
            </div>
            <div className={"col-4"}>
                <img src={img} alt={title}/>
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

    return <div id={"download-component"}>
        <PageHeader title={"Download"} subtitle={"Download table data to CSV files"}/>

        {siteResponse?.hasSource && (
            <>
                <Section title={"Structured Download"}
                         description={"Select multiple tables and the dependencies established based database constraints, and filter the data you want to download."}
                         to={"/download/structured"}
                         img={structured_download}/>
            </>
        )}

        {siteResponse?.hasSource && (
            <Section title={"Bulk Download"}
                     description={"Select multiple tables and download the full content."}
                     to={"/download/bulk"}
                     img={bulk_download}/>
        )}

        {siteResponse?.hasSource && siteResponse?.hasTarget && <hr/>}

        {siteResponse?.hasTarget && (
            <Section title={"Full Download"}
                     description={<>Dump the full content of your TARGET database back to CSV files.<br/>
                         This might be useful if you use your application to generate additional data and want to save the result to CSV files.</>}
                     to={"/download/target"}
                     img={full_download}
            />
        )}
    </div>;
}