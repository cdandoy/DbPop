import {useEffect, useState} from "react";
import axios from "axios";

export interface ExecutionStatus {
    name: string;
    running: boolean;
    error?: string;
}

export interface SiteStatus {
    statuses: ExecutionStatus[];
    complete: boolean;
}

export function useSetupStatusEffect(setSiteStatus: ((setSiteStatus: SiteStatus) => void)) {
    const [changeNumber, setChangeNumber] = useState(0)

    const updateSetupState = (siteStatusResponse: SiteStatus) => {
        setSiteStatus(siteStatusResponse);
        if (!siteStatusResponse.complete) {
            setTimeout(() => {
                setChangeNumber(changeNumber + 1);
            }, 2000);
        }
    };

    useEffect(() => {
        axios.get<SiteStatus>('/status')
            .then(result => {
                updateSetupState(result.data);
            })
            .catch(() => {
                updateSetupState({
                    statuses: [
                        {name: "Connecting to the server", running: true}
                    ],
                    complete: false,
                });
            })
    }, [changeNumber])
}