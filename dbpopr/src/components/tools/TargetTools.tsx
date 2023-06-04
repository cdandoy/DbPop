import React, {useContext} from "react";
import PageHeader from "../pageheader/PageHeader";
import {Section} from "./Section";
import target_data from "./target_data.png"
import target_code from "./target_code.png"
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
                         img={target_data}/>

                <Section title={"Code"}
                         description={"Tools that apply to code"}
                         to={"/tools/target/code"}
                         img={target_code}/>
            </div>
        </div>
    </div>
}

