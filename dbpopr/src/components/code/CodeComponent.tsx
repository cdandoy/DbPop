import React, {useEffect, useState} from "react";
import PageHeader from "../pageheader/PageHeader";
import {NavLink} from "react-router-dom";
import {siteApi, SiteResponse} from "../../api/siteApi";
import compare_source from "./compare_source.png"
import download_source from "./download_source.png"
import upload_target from "./upload_target.png"


function Section({title, description, to, img}: {
    title: string;
    description: string | JSX.Element;
    to: string;
    img?: any;
}) {
    return <>
        <div className={"code-component-section row mt-5"}>
            <div className={"col-8"}>
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
                         description={"Compare the source database with the local files"}
                         to={"/code/source/compare"}
                         img={compare_source}
                />
            )}

            {siteResponse?.hasSource && (
                <Section title={"Download Source"}
                         description={"Download the source database to the local filesystem"}
                         to={"/code/source/download"}
                         img={download_source}
                />
            )}

            {siteResponse?.hasTarget && (
                <Section title={"Upload to Target"}
                         description={"Create"}
                         to={"/code/target/upload"}
                         img={upload_target}
                />
            )}

        </div>
    </div>
}