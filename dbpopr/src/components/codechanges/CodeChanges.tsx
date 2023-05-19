import React, {useEffect, useState} from "react";
import {Button} from "react-bootstrap";
import './CodeChanges.scss'
import PageHeader from "../pageheader/PageHeader";
import useWebSocket from "react-use-websocket";
import {Change, DefaultChanges, targetChanges} from "../../api/changeApi";
import {uploadDbChangeToTarget, uploadFileChangeToTarget} from "../../api/codeApi";
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
        uploadFileChangeToTarget([objectIdentifier])
            .then(() => refreshChanges());
    }

    function onApplyDbChanges(path: string, objectIdentifier: ObjectIdentifier) {
        uploadDbChangeToTarget([objectIdentifier])
            .then(() => refreshChanges());
    }

    function onApplyAllFileChanges() {
        const applyChanges = changes.map(value => value.objectIdentifier);
        uploadFileChangeToTarget(applyChanges)
            .then(() => refreshChanges());
    }

    function onApplyAllDbChanges() {
        const applyChanges = changes.map(value => value.objectIdentifier);
        uploadDbChangeToTarget(applyChanges)
            .then(() => refreshChanges());
    }


    function ObjectIdentifierIcon({objectIdentifier}: { objectIdentifier: ObjectIdentifier }) {
        function icon(type: string) {
            if (type === 'SQL_STORED_PROCEDURE') return 'fa-code'
            if (type === 'USER_TABLE') return 'fa-table'
            return ''
        }

        function title(type: string) {
            if (type === 'SQL_STORED_PROCEDURE') return 'Stored Procedure'
            if (type === 'USER_TABLE') return 'Table'
            return ''
        }

        return <>
            <i className={`fa fa-fw ${icon(objectIdentifier.type)}`} title={title(objectIdentifier.type)}></i>&nbsp;
        </>
    }


    function ApplyAllBox() {
        return <>
            <div className="card">
                <div className="card-body">
                    <table>
                        <tbody>
                        {changes.filter(it => it.databaseChanged || it.databaseDeleted).length > 0 &&
                            <tr>
                                <td>
                                    <Button variant={"primary"}
                                            size={"sm"}
                                            onClick={() => onApplyAllDbChanges()}
                                            className={"me-3"}
                                    >
                                        <i className={"fa fa-file"} style={{paddingRight: "3px"}}/>
                                        <i className={"fa fa-arrow-left"} style={{paddingRight: "3px"}}/>
                                        <i className={"fa fa-database"}/>
                                    </Button>
                                </td>
                                <td><strong>Apply All Database Changes</strong></td>
                            </tr>
                        }
                        {changes.filter(it => it.fileChanged || it.fileDeleted).length > 0 &&
                            <tr>
                                <td>
                                    <Button variant={"outline-primary"}
                                            size={"sm"}
                                            onClick={() => onApplyAllFileChanges()}
                                            className={"me-2 mt-2"}
                                    >
                                        <i className={"fa fa-file"} style={{paddingRight: "3px"}}/>
                                        <i className={"fa fa-arrow-right"} style={{paddingRight: "3px"}}/>
                                        <i className={"fa fa-database"}/>
                                    </Button>
                                </td>
                                <td><strong>Apply All File Changes</strong></td>
                            </tr>
                        }
                        </tbody>
                    </table>
                </div>
            </div>
        </>
    }

    function ChangesBox() {
        return <>
            <div className="card mt-5">
                <div className="card-body">
                    <table className={"table table-hover"}>
                        <tbody>
                        {changes.map(change => <tr key={change.objectIdentifier.type + '-' + change.dbname}>
                            <td width={"100%"}>
                                <ObjectIdentifierIcon objectIdentifier={change.objectIdentifier}/>
                                {change.dbname}
                            </td>
                            <td width={"0"} style={{whiteSpace: "nowrap"}}>
                                {(change.fileChanged || change.fileDeleted) && <>
                                    <Button variant={"outline-primary"}
                                            size={"sm"}
                                            title={"Apply the source file change to the database"}
                                            onClick={() => onApplyFileChanges(change.path, change.objectIdentifier)}
                                    >
                                        <i className={"fa fa-file"} style={{paddingRight: "3px"}}/>
                                        <i className={"fa fa-arrow-right"} style={{paddingRight: "3px"}}/>
                                        <i className={"fa fa-database"}/>
                                    </Button>
                                </>
                                }
                                {(change.databaseChanged || change.databaseDeleted) && <>
                                    <Button variant={"primary"}
                                            size={"sm"}
                                            title={"Apply the database change to the source file"}
                                            onClick={() => onApplyDbChanges(change.path, change.objectIdentifier)}
                                    >
                                        <i className={"fa fa-file"} style={{paddingRight: "3px"}}/>
                                        <i className={"fa fa-arrow-left"} style={{paddingRight: "3px"}}/>
                                        <i className={"fa fa-database"}/>
                                    </Button>
                                </>}
                            </td>
                        </tr>)}
                        </tbody>
                    </table>
                </div>
            </div>
        </>
    }

    return <div className={"code-changes-component"}>
        <PageHeader title={"Code Changes"} subtitle={"Track Modified database code"} tool={
            <div title={"This section is still under construction"}>
                <i className={"fa fa-warning"}/>
                Experimental
            </div>}
        />

        {changes.length > 0 && <>
            <div className={"row"}>
                <div className={"col-6"}>
                    <ApplyAllBox/>
                </div>
            </div>

            <ChangesBox/>
        </>}
        {changes.length === 0 && <>
            <h5 className={"text-center"}>
                No Changes Detected
            </h5>
        </>
        }
    </div>
}