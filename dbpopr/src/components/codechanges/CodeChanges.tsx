import React, {useContext, useEffect, useState} from "react";
import {Button} from "react-bootstrap";
import './CodeChanges.scss'
import PageHeader from "../pageheader/PageHeader";
import {ObjectIdentifier} from "../../models/ObjectIdentifier";
import {WebSocketStateContext} from "../ws/useWebSocketState";
import {NavLink} from "react-router-dom";
import axios from "axios";

function Message({message}: { message: string }) {
    return <>
        <h5 className={"text-center"}>
            {message}
        </h5>
    </>
}

export interface ChangedObject {
    objectIdentifier: ObjectIdentifier;
    name: string;
    type: string;
    changeType: string;
}

export default function CodeChanges() {
    const siteStatus = useContext(WebSocketStateContext);
    const [changedObjects, setChangedObjects] = useState<ChangedObject[]>([]);
    const [refreshCount, setRefreshCount] = useState(0)

    useEffect(() => {
        axios.get<ChangedObject[]>(`/codechanges/target`)
            .then(response => {
                setChangedObjects(response.data);
            });
    }, [siteStatus.codeDiffChanges, refreshCount]);

    function refresh() {
        setRefreshCount(refreshCount + 1);
    }

    function onApplyFileChange(objectIdentifier: ObjectIdentifier) {
        axios.post(`/codechanges/target/apply-file`, objectIdentifier)
            .then(refresh);
    }

    function onApplyDbChange(objectIdentifier: ObjectIdentifier) {
        axios.post(`/codechanges/target/apply-db`, objectIdentifier)
            .then(refresh);
    }

    function onApplyAllFileChanges() {
        axios.post(`/codechanges/target/apply-all-files`)
            .then(refresh);
    }

    function onApplyAllDbChanges() {
        axios.post(`/codechanges/target/apply-all-db`)
            .then(refresh);
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
                        {changedObjects.filter(change => change.changeType === "DATABASE_ONLY" || change.changeType === "UPDATED").length > 0 &&
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
                        {changedObjects.filter(change => change.changeType === "FILE_ONLY" || change.changeType === "UPDATED").length > 0 &&
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
                        {changedObjects.map(change => <tr key={change.objectIdentifier.type + '-' + change.name}>
                            <td width={"100%"}>
                                <NavLink to={"/codechanges/diff"} state={change.objectIdentifier}>
                                    <ObjectIdentifierIcon objectIdentifier={change.objectIdentifier}/>
                                    {change.name}
                                </NavLink>
                            </td>
                            <td width={"0"} style={{whiteSpace: "nowrap"}}>
                                {(change.changeType === "FILE_ONLY" || change.changeType === "UPDATED") && <>
                                    <Button variant={"outline-primary"}
                                            size={"sm"}
                                            title={"Apply the source file change to the database"}
                                            onClick={() => onApplyFileChange(change.objectIdentifier)}
                                    >
                                        <i className={"fa fa-file"} style={{paddingRight: "3px"}}/>
                                        <i className={"fa fa-arrow-right"} style={{paddingRight: "3px"}}/>
                                        <i className={"fa fa-database"}/>
                                    </Button>
                                </>
                                }
                                {(change.changeType === "DATABASE_ONLY" || change.changeType === "UPDATED") && <>
                                    <Button variant={"primary"}
                                            size={"sm"}
                                            title={"Apply the database change to the source file"}
                                            onClick={() => onApplyDbChange(change.objectIdentifier)}
                                    >
                                        <i className={"fa fa-file"} style={{paddingRight: "3px"}}/>
                                        <i className={"fa fa-arrow-left"} style={{paddingRight: "3px"}}/>
                                        <i className={"fa fa-database"}/>
                                    </Button>
                                </>}
                            </td>
                        </tr>)}
                        {changedObjects.length >= 100 && <tr>
                            <td colSpan={2}><Message message={"Too many changes to display"}/></td>
                        </tr>
                        }
                        </tbody>
                    </table>
                </div>
            </div>
        </>
    }

    function Content() {
        if (!siteStatus.codeScanComplete) return <>
            <h5 className={"text-center"}>
                <i className={"fa fa-spinner fa-spin"}/>
                Scanning
            </h5>
        </>
        if (changedObjects.length === 0) return <Message message={"No Changes Detected"}/>;
        return <>
            <div className={"row"}>
                <div className={"col-6"}>
                    <ApplyAllBox/>
                </div>
            </div>
            <ChangesBox/>
        </>
    }

    return <div className={"code-changes-component container"}>
        <PageHeader title={"Code Changes"} subtitle={"Track modified database code"}/>
        <Content/>
    </div>
}