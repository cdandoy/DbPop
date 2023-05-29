import React, {useContext} from "react";
import PageHeader from "../pageheader/PageHeader";
import {NavLink} from "react-router-dom";
import compare_source from "./code/sourcecompare/compare_source.png"
import download_source from "./code/sourcedownload/download_source.png"
import upload_target from "./code/targetupload/upload_target.png"
import download_target from "./code/targetdownload/download_target.png"
import compare_target from "./code/targetcompare/compare_target.png"
import {SiteContext} from "../app/App";
import structured_download from "./data/source_structured/source_structured.png"
import bulk_download from "./data/source_bulk/source_bulk.png"
import target_full from "./data/target_full/target_full.png"
import source_full from "./data/source_full/source_full.png"
import "./ToolsComponent.scss"

function Section({title, description, to, img, disabled}: {
    title: string;
    description: string | JSX.Element;
    to: string;
    img?: any;
    disabled?: boolean;
}) {
    return <>
        <div className={"code-component-section row mt-5"}>
            <div className={"col-8"}>
                <h3>{title}</h3>
                <p>{description}</p>
                <div className={"ms-5 button-bar"}>
                    <NavLink to={to}>
                        <button className={"btn btn-primary"} disabled={disabled}>
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

export default function ToolsComponent() {
    const siteResponse = useContext(SiteContext);

    return <div id={"tools-component"} className={"container"}>
        <div>
            <PageHeader title={"Tools"} subtitle={"Download and upload code and data"}/>

            {siteResponse?.hasSource && (
                <>
                    <h1>Data - Source</h1>
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
                </>
            )}

            {siteResponse?.hasTarget && (
                <>
                    <h1 className={"mt-5"}>Data - Target</h1>
                    <Section title={"Full Target Download"}
                             description={<>Dump the full content of your TARGET database back to CSV files.<br/>
                                 This might be useful if you use your application to generate additional data and want to save the result to CSV files.</>}
                             to={"/tools/target/full"}
                             img={target_full}
                    />
                </>
            )}

            {siteResponse?.hasSource && (
                <>
                    <h1 className={"mt-5"}>Code - Source</h1>
                    <Section title={"Compare Source"}
                             description={"Compare the tables and sprocs in the source database with the local SQL files"}
                             to={"/tools/source/compare"}
                             img={compare_source}
                    />
                    <Section title={"Download Source"}
                             description={"Download the tables and sprocs from the source database to the local SQL files"}
                             to={"/tools/source/download"}
                             img={download_source}
                    />
                </>
            )}

            {siteResponse?.hasTarget && (
                <>
                    <h1 className={"mt-5"}>Code - Target</h1>
                    <Section title={"Compare Target"}
                             description={"Compare the tables and sprocs in the Target database with the local SQL files"}
                             to={"/tools/target/compare"}
                             img={compare_target}
                    />
                    <Section title={"Upload to Target"}
                             description={"Upload the tables and sprocs from the SQL files to the Target database"}
                             to={"/tools/target/upload"}
                             img={upload_target}
                    />
                    <Section title={"Download from Target"}
                             description={"Download the tables and sprocs from the Target database to the SQL files"}
                             to={"/tools/target/download"}
                             img={download_target}
                    />
                </>
            )}

        </div>
    </div>
}