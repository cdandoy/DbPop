import React, {useContext} from "react";
import PageHeader from "../pageheader/PageHeader";
import {Section} from "./Section";
import source_full from "./data/source_full/source_full.png"
import structured_download from "./data/source_structured/source_structured.png";
import bulk_download from "./data/source_bulk/source_bulk.png";
import {SiteStatusContext} from "../app/App";

export default function SourceDataTools() {
    const siteStatus = useContext(SiteStatusContext);
    return <div id={"tools-component"} className={"container"}>
        <div>
            <PageHeader title={"Tools"}
                        breadcrumbs={
                            siteStatus.hasTarget ? [
                                    {to: "/tools", label: "Tools"},
                                    {to: "/tools/source", label: "Source"},
                                    {label: "Data"},
                                ] :
                                [
                                    {to: "/tools/source", label: "Tools"},
                                    {label: "Data"},
                                ]
                        }
            />

            <div className={"ms-8"}>
                <Section title={"Structured Download"}
                         description={"Select multiple tables and the dependencies established based database constraints, and filter the data you want to download."}
                         to={"/tools/source/structured"}
                         img={structured_download}/>

                <Section title={"Bulk Download"}
                         description={"Select multiple tables and download the full content."}
                         to={"/tools/source/bulk"}
                         img={bulk_download}/>
                <Section title={"Full Download"}
                         description={"Download a complete database. Use this option if you already have a seeded test database that you want to convert to DbPop."}
                         to={"/tools/source/full"}
                         img={source_full}/>
            </div>
        </div>
    </div>
}

