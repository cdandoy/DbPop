import React, {useEffect, useState} from "react";
import {Button} from "react-bootstrap";
import './CodeChanges.scss'
import PageHeader from "../pageheader/PageHeader";
import useWebSocket from "react-use-websocket";
import {Change, DefaultChanges, targetChanges} from "../../api/changeApi";
import {ApplyChange, uploadDbChangeToTarget, uploadFileChangeToTarget} from "../../api/codeApi";
import {ObjectIdentifier} from "../../models/ObjectIdentifier";

const WS_URL = 'ws://localhost:8080/ws/site';

interface Message {
    messageType: string
}

export function useCodeChanges() {
    const [changes, setChanges] = useState<Change[]>(DefaultChanges);
    const {lastJsonMessage} = useWebSocket(
        WS_URL,
        {
            shouldReconnect: () => true,
            reconnectAttempts: 10,
            reconnectInterval: (attemptNumber) =>
                Math.min(Math.pow(2, attemptNumber) * 1000, 10000),
        }
    );

    function refreshChanges() {
        targetChanges()
            .then(result => {
                setChanges(result.data);
            })
    }

    useEffect(() => refreshChanges(), []);

    useEffect(() => {
        if (lastJsonMessage) {
            console.log(lastJsonMessage)
            const message = lastJsonMessage as any as Message;
            if (message.messageType === 'CODE_CHANGE') {
                targetChanges()
                    .then(result => {
                        setChanges(result.data);
                    })
            }
        }
    }, [lastJsonMessage]);

    return {
        changes,
        refreshChanges
    }
}

export default function CodeChanges() {
    const {changes, refreshChanges} = useCodeChanges();

    function onApplyFileChanges(path: string, objectIdentifier: ObjectIdentifier) {
        uploadFileChangeToTarget([{path, objectIdentifier}])
            .then(() => refreshChanges());
    }

    function onApplyDbChanges(path: string, objectIdentifier: ObjectIdentifier) {
        uploadDbChangeToTarget([{path, objectIdentifier}])
            .then(() => refreshChanges());
    }

    function onApplyAllFileChanges() {
        const applyChanges = changes.map(value => ({path: value.path, objectIdentifier: value.objectIdentifier} as ApplyChange));
        uploadFileChangeToTarget(applyChanges)
            .then(() => refreshChanges());
    }

    function onApplyAllDbChanges() {
        const applyChanges = changes.map(value => ({path: value.path, objectIdentifier: value.objectIdentifier} as ApplyChange));
        uploadDbChangeToTarget(applyChanges)
            .then(() => refreshChanges());
    }

    return <div className={"code-changes-component"}>
        <PageHeader title={"Code Changes"} subtitle={"Track Modified database code"} tool={
            <div title={"This section is still under construction"}>
                <i className={"fa fa-warning"}/>
                Experimental
            </div>}
        />

        <div className={"changes"}>
            <div className={"row mt-1"}>
                <div className={"col-5"}><h5>Source File</h5></div>
                <div className={"col-2 column-buttons text-center"}>
                    {/* Apply All Left */}
                    <Button variant={"primary"}
                            style={changes.filter(it => it.databaseChanged).length ? {} : {visibility: "hidden"}}
                            title={"Apply all the database change to the source file"}
                            onClick={() => onApplyAllDbChanges()}
                    >
                        <i className={"fa fa-arrow-left"}/>
                    </Button>

                    {/*Spacer*/}
                    <Button variant={"primary"}
                            size={"sm"}
                            style={{visibility: "hidden"}}
                    >
                        <i className={"fa fa-code-compare"}/>
                    </Button>

                    {/* Apply Right */}
                    <Button variant={"primary"}
                            size={"sm"}
                            style={changes.filter(it => it.fileChanged).length ? {} : {visibility: "hidden"}}
                            title={"Apply all the source file change to the database"}
                            onClick={() => onApplyAllFileChanges()}
                    >
                        <i className={"fa fa-arrow-right"}/>
                    </Button>

                </div>
                <div className={"col-5"}><h5>Database Object</h5></div>
            </div>

            <div>
                {changes.map(change => (
                    <div key={change.path} className={"row mt-1"}>
                        <div className={"col-5 column-path"} title={change.path}>{change.path}</div>
                        <div className={"col-2 column-buttons"}>
                            {/* Apply Left */}
                            <Button variant={"primary"}
                                    size={"sm"}
                                    style={change.databaseChanged ? {} : {visibility: "hidden"}}
                                    title={"Apply the database change to the source file"}
                                    onClick={() => onApplyDbChanges(change.path, change.objectIdentifier)}
                            >
                                <i className={"fa fa-arrow-left"}/>
                            </Button>

                            {/*Compare*/}
                            <Button variant={"primary"}
                                    size={"sm"}
                                    style={false /*change.databaseChanged || change.fileChanged*/ ? {} : {visibility: "hidden"}}
                                    title={"Compare"}
                                    onClick={() => onApplyFileChanges(change.path, change.objectIdentifier)}
                            >
                                <i className={"fa fa-code-compare"}/>
                            </Button>

                            {/* Apply Right */}
                            <Button variant={"primary"}
                                    size={"sm"}
                                    style={change.fileChanged ? {} : {visibility: "hidden"}}
                                    title={"Apply the source file change to the database"}
                                    onClick={() => onApplyFileChanges(change.path, change.objectIdentifier)}
                            >
                                <i className={"fa fa-arrow-right"}/>
                            </Button>
                        </div>
                        <div className={"col-5 column-db"} title={change.dbname}>{change.dbname}</div>
                    </div>
                ))
                }
            </div>
        </div>
    </div>;
}