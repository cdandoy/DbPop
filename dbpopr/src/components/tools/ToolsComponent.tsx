import React from "react";
import PageHeader from "../pageheader/PageHeader";
import source_full from "./data/source_full/source_full.png"
import structured_download from "./data/source_structured/source_structured.png";
import bulk_download from "./data/source_bulk/source_bulk.png";
import "./ToolsComponent.scss"
import {Section} from "./Section";
import target_full from "./data/target_full/target_full.png";

export default function ToolsComponent() {
    return <div id={"tools-component"} className={"container"}>
        <div>
            <PageHeader title={"Tools"} subtitle={"Download and upload code and data"}/>

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
                <Section title={"Target Download"}
                         description={<>Dump the full content of your TARGET database back to CSV files.<br/>
                             This might be useful if you use your application to generate additional data and want to save the result to CSV files.</>}
                         to={"/tools/target/full"}
                         img={target_full}
                />
            </div>
        </div>
    </div>
}