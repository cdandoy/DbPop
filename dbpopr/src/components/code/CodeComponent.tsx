import React, {useEffect, useState} from "react";
import PageHeader from "../pageheader/PageHeader";
import {NavLink} from "react-router-dom";
import {siteApi, SiteResponse} from "../../api/siteApi";
import compare_source from "./compare_source.png"
import download_source from "./download_source.png"
import upload_target from "./upload_target.png"
import download_target from "./download_target.png"
import compare_target from "./compare_target.png"

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

export default function CodeComponent() {
    const [siteResponse, setSiteResponse] = useState<SiteResponse | undefined>();

    useEffect(() => {   // I should use context or redux for that
        siteApi()
            .then(result => {
                setSiteResponse(result.data);
            })
    }, []);

    return <div id={"code-component"}>
        <div>
            <PageHeader title={"Code"} subtitle={"Tables and Stored Procedures"}/>

            {siteResponse?.hasSource && (
                <Section title={"Compare Source"}
                         description={"Compare the tables and sprocs in the source database with the local SQL files"}
                         to={"/code/source/compare"}
                         img={compare_source}
                />
            )}

            {siteResponse?.hasSource && (
                <Section title={"Download Source"}
                         description={"Download the tables and sprocs from the source database to the local SQL files"}
                         to={"/code/source/download"}
                         img={download_source}
                />
            )}

            {siteResponse?.hasSource && siteResponse?.hasTarget && <hr/>}

            {siteResponse?.hasTarget && (
                <Section title={"Compare Target"}
                         description={"Compare the tables and sprocs in the Target database with the local SQL files"}
                         to={"/code/target/compare"}
                         img={compare_target}
                />
            )}

            {siteResponse?.hasTarget && (
                <Section title={"Upload to Target"}
                         description={"Upload the tables and sprocs from the SQL files to the Target database"}
                         to={"/code/target/upload"}
                         img={upload_target}
                />
            )}

            {siteResponse?.hasTarget && (
                <Section title={"Download from Target"}
                         description={"Download the tables and sprocs from the Target database to the SQL files"}
                         to={"/code/target/download"}
                         img={download_target}
                />
            )}

        </div>
    </div>
}