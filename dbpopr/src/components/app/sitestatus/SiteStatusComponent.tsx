import React from "react";
import {ExecutionStatus, SiteStatus} from "./useSetupStatusEffect";

export default function SiteStatusComponent({siteStatus}: { siteStatus: SiteStatus }) {
    function StatusLine({s, i}: { s: ExecutionStatus, i: number }) {
        return (
            <div>
                <div key={i}>
                    <i className={`fa fa-fw ${s.running ? 'fa-spinner fa-spin' : 'fa-check'}`}/> {s.name}
                </div>
                {s.error && (
                    <div className={"alert alert-danger"}>
                        {s.error}
                    </div>
                )}
            </div>
        );
    }

    return (
        <>
            <div className={"ms-5 mt-5"}>
                <h1>Loading</h1>
                <div className={"ms-3 mt-3"}>
                    {siteStatus.statuses.length
                        ? siteStatus.statuses.map((s, i) => <StatusLine s={s} i={i}/>)
                        : <><i className={"fa fa-fw fa-spinner fa-spin"}/> Initializing</>
                    }
                </div>
            </div>
        </>
    );
}