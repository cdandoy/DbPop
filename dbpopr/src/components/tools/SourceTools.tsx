import React, {useContext} from "react";
import PageHeader from "../pageheader/PageHeader";
import {Section} from "./Section";
import source_full from "./data/source_full/source_full.png"
import download_source from "./code/sourcedownload/download_source.png"
import {SiteContext} from "../app/App";

export default function SourceTools() {
    const siteResponse = useContext(SiteContext);
    return <div id={"tools-component"} className={"container"}>
        <div>
            <PageHeader title={"Tools"}
                        breadcrumbs={
                            siteResponse.hasTarget ? [
                                    {to: "/tools", label: "Tools"},
                                    {label: "Source"},
                                ] :
                                undefined
                        }
            />

            <div className={"ms-8"}>
                <Section title={"Data"}
                         description={"Tools that apply data"}
                         to={"/tools/source/data"}
                         img={source_full}/>

                <Section title={"Code"}
                         description={"Tools that apply to code"}
                         to={"/tools/source/code"}
                         img={download_source}/>
            </div>
        </div>
    </div>
}

