import React, {useContext} from "react";
import PageHeader from "../pageheader/PageHeader";
import {Section} from "./Section";
import target_full from "./data/target_full/target_full.png"
import download_target from "./code/targetdownload/download_target.png"
import {SiteContext} from "../app/App";

export default function TargetTools() {
    const siteResponse = useContext(SiteContext);
    return <div id={"tools-component"} className={"container"}>
        <div>
            <PageHeader title={"Tools"}
                        breadcrumbs={
                            siteResponse.hasSource ? [
                                    {to: "/tools", label: "Tools"},
                                    {label: "Target"},
                                ] :
                                undefined
                        }
            />

            <div className={"ms-8"}>
                <Section title={"Data"}
                         description={"Tools that apply data"}
                         to={"/tools/target/data"}
                         img={target_full}/>

                <Section title={"Code"}
                         description={"Tools that apply to code"}
                         to={"/tools/target/code"}
                         img={download_target}/>
            </div>
        </div>
    </div>
}

