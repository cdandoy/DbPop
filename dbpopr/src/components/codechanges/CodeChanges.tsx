import React, {useContext, useEffect, useState} from "react";
import {Button} from "react-bootstrap";
import './CodeChanges.scss'
import PageHeader from "../pageheader/PageHeader";
import {ObjectIdentifier} from "../../models/ObjectIdentifier";
import {WebSocketStateContext} from "../ws/useWebSocketState";
import {NavLink} from "react-router-dom";
import axios from "axios";
import LoadingOverlay from "../utils/LoadingOverlay";

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
    const [refreshCount, setRefreshCount] = useState(0);
    const [loading, setLoading] = useState(false);
    const [errors, setErrors] = useState<string[]>([]);

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
        setErrors([]);
        setLoading(true);
        axios.post(`/codechanges/target/apply-file`, objectIdentifier)
            .then((response) => {
                const errors = response.data.fileExecutions
                        .map((it: any) => it.error)
                        .filter((it: any) => !!it);
                setErrors(errors);
                refresh();
            })
            .finally(() => setLoading(false));
    }

    function onApplyDbChange(objectIdentifier: ObjectIdentifier) {
        setErrors([]);
        setLoading(true);
        axios.post(`/codechanges/target/apply-db`, objectIdentifier)
            .then(refresh)
            .finally(() => setLoading(false));
    }

    function onApplyAllFileChanges() {
        setErrors([]);
        setLoading(true);
        axios.post(`/codechanges/target/apply-all-files`)
            .then(refresh)
            .finally(() => setLoading(false));
    }

    function onApplyAllDbChanges() {
        setErrors([]);
        setLoading(true);
        axios.post(`/codechanges/target/apply-all-db`)
            .then(refresh)
            .finally(() => setLoading(false));
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

    function Footer() {
        // If there is only one change, no need for an "Apply All"
        if (changedObjects.length <= 1) return <></>

        const canUploadAllFiles = changedObjects.filter(change => (change.changeType === "UPDATED" || change.changeType === "FILE_ONLY")).length > 0;
        const canUploadAllDatabase = changedObjects.filter(change => (change.changeType === "UPDATED" || change.changeType === "DATABASE_ONLY")).length > 0;

        // We can only apply-all when there are updates, not when the SQL is only in files or only in database
        if (!(canUploadAllFiles || canUploadAllDatabase)) {
            return <></>
        }

        return <tfoot>
        <tr>
            <td style={{width: "100%"}} className={"object-name"}>
                <strong>All</strong>
            </td>
            <td style={{minWidth: '20em'}} className={"text-center"}>
                {canUploadAllFiles &&
                    <Button variant={"primary"}
                            size={"sm"}
                            className={"apply-button"}
                            onClick={() => onApplyAllFileChanges()}
                    >
                        Upload All <i className={"fa fa-arrow-right"}/>
                    </Button>
                }
            </td>
            <td style={{minWidth: '20em'}} className={"text-center"}>
                {canUploadAllDatabase &&
                    <Button variant={"primary"}
                            size={"sm"}
                            className={"apply-button"}
                            onClick={() => onApplyAllDbChanges()}
                    >
                        <i className={"fa fa-arrow-left"}/> Download All
                    </Button>
                }
            </td>
        </tr>
        </tfoot>
    }

    function ChangesBox() {

        function Changes({change}: { change: ChangedObject }) {
            const objectIdentifier = change.objectIdentifier;
            let applyOptions = 1;

            return <>
                <td style={{minWidth: '20em'}} className={"text-center"}>
                    {change.changeType === "FILE_ONLY" &&
                        <Button variant={"danger"}
                                size={"sm"}
                                className={"apply-button"}
                                onClick={() => onApplyDbChange(objectIdentifier)}
                        >
                            <i className={"fa fa-trash"}/> Delete
                        </Button>
                    }

                    {/*
                        If we consider that the database is the reference, the only operations should be:
                            * download the database code if it is different
                            * Delete the local file if the database code is missing.
                        But then we don't have a good option when the file is newer
                    */}
                    {applyOptions === 1 ? <>
                            {(change.changeType === "FILE_ONLY" || change.changeType === "FILE_NEWER" || change.changeType === "UPDATED") &&
                                <Button variant={"primary"}
                                        size={"sm"}
                                        className={"apply-button"}
                                        onClick={() => onApplyFileChange(objectIdentifier)}
                                >
                                    Upload <i className={"fa fa-arrow-right"}/>
                                </Button>
                            }
                        </> :
                        <>
                            {(change.changeType === "FILE_NEWER" || change.changeType === "UPDATED") &&
                                <Button variant={"primary"}
                                        size={"sm"}
                                        className={"apply-button"}
                                        onClick={() => onApplyFileChange(objectIdentifier)}
                                >
                                    Upload <i className={"fa fa-arrow-right"}/>
                                </Button>
                            }
                        </>
                    }
                </td>
                <td style={{minWidth: '20em'}} className={"text-center"}>
                    {(change.changeType === "DATABASE_ONLY" || change.changeType === "DATABASE_NEWER" || change.changeType === "UPDATED") &&
                        <Button variant={"primary"}
                                size={"sm"}
                                className={"apply-button"}
                                onClick={() => onApplyDbChange(objectIdentifier)}
                        >
                            <i className={"fa fa-arrow-left"}/> Download
                        </Button>
                    }
                </td>
            </>
        }

        function changeRow(change: ChangedObject) {
            return <tr key={change.objectIdentifier.type + '-' + change.name}>
                <td width={"80%"} className={"object-name"}>
                    <NavLink to={"/codechanges/diff"} state={change.objectIdentifier}>
                        <ObjectIdentifierIcon objectIdentifier={change.objectIdentifier}/>
                        {change.name}
                    </NavLink>
                </td>
                <Changes change={change}/>
            </tr>
        }

        return <div className={"changes-box"}>
            {errors.map((error, i) => <div key={`error-${i}`} className="alert alert-danger" role="alert">{error}</div>)}
            <table className={"table table-hover"}>
                <thead>
                <tr>
                    <th>Object</th>
                    <th style={{minWidth: '20em'}} className={'text-center'}>File</th>
                    <th style={{minWidth: '20em'}} className={'text-center'}>Database</th>
                </tr>
                </thead>
                <tbody>
                {changedObjects.map(changeRow)}
                {changedObjects.length >= 100 && <tr>
                    <td colSpan={2}><Message message={"Too many changes to display"}/></td>
                </tr>
                }
                </tbody>
                <Footer/>
            </table>
        </div>
    }

    function Scanning() {
        return <h5 className={"text-center"}>
            <i className={"fa fa-spinner fa-spin"}/> Scanning
        </h5>
    }

    function NoChanges() {
        return <Message message={"No Changes Detected"}/>;
    }

    function Content() {
        if (!siteStatus.codeScanComplete) return <Scanning/>
        if (changedObjects.length === 0) return <NoChanges/>
        return <ChangesBox/>
    }

    function Tool() {
        return (
            <NavLink className={"btn btn-sm btn-primary"} to={"/deployment"}>
                <i className={"fa fa-rocket"}/> Deployment...
            </NavLink>
        )
    }

    return <div className={"code-changes-component container"}>
        <LoadingOverlay active={loading}/>
        <PageHeader title={"Code Changes"} subtitle={"Track modified database code."} tool={<Tool/>}/>
        <Content/>
    </div>
}