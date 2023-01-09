import {SetupState} from "./SetupState";
import {useEffect, useState} from "react";
import axios from "axios";

export function useSetupStatusEffect(setSetupState:((setupState:SetupState)=>void)) {
    const [changeNumber, setChangeNumber] = useState(0)
    function updateSetupState(setupState: SetupState) {
        setSetupState(setupState);
        if (setupState.activity) {
            setTimeout(() => {
                setChangeNumber(changeNumber + 1);
            }, 2000);
        }
    }

    useEffect(() => {
        axios.get<SetupState>('/site/status')
            .then(result => {
                updateSetupState(result.data);
            })
            .catch(error => {
                updateSetupState({
                    activity: "Load State",
                    error: error?.message || 'Internal Error',
                });
            })
    }, [changeNumber])
}