import React, {useEffect, useState} from "react";
import PageHeader from "../pageheader/PageHeader";
import LoadingOverlay from "../utils/LoadingOverlay";
import {CodeDiff, CodeDiffEntry, compareTargetToFile} from "../../api/codeApi";
import {Alert, Button} from "react-bootstrap";
import {tableNameToFqName} from "../../models/TableName";
import "./CodeSourceCompare.scss"
import compare_target from "./compare_target.png"

export default function CodeTargetCompare() {
    const [loading, setLoading] = useState(false);
    const [codeDiff, setCodeDiff] = useState<CodeDiff | undefined>();
    const [error, setError] = useState<string | undefined>();

    useEffect(() => {
        setLoading(true);
        setError(undefined);
        compareTargetToFile()
            .then(result => setCodeDiff(result.data))
            .catch((error) => setError(error))
            .finally(() => setLoading(false));
    }, [])

    function getFileStatus(codeDiffEntry: CodeDiffEntry) {
        const fileTime = codeDiffEntry.fileTime;
        if (!fileTime) return "";
        return new Date(fileTime).toLocaleString();
    }

    function getDirection(codeDiffEntry: CodeDiffEntry) {
        const databaseTime = codeDiffEntry.databaseTime;
        const fileTime = codeDiffEntry.fileTime;
        if (!fileTime) return "left";
        if (!databaseTime) return "right";
        if (fileTime < databaseTime) return "left";
        if (fileTime > databaseTime) return "right";
        return "";
    }

    function getButton(codeDiffEntry: CodeDiffEntry) {
        let direction = getDirection(codeDiffEntry);
        if (direction === "left") return <Button size={"sm"}> <i className={"fa fa-arrow-left"}/> </Button>
        if (direction === "right") return <Button size={"sm"}> <i className={"fa fa-arrow-right"}/> </Button>
        return <></>
    }

    function getDatabaseStatus(codeDiffEntry: CodeDiffEntry) {
        const databaseTime = codeDiffEntry.databaseTime;
        if (!databaseTime) return "";
        return new Date(databaseTime).toLocaleString();
    }

    return <div id={"code-target-compare"}>
        <PageHeader title={"Compare Target"} subtitle={"Compare the tables and sprocs in the Target database with the local SQL files"} tool={<img src={compare_target} style={{width: "20em"}} alt={"Compare Target"}/>}/>
        <LoadingOverlay active={loading}/>
        {error && <Alert variant={"danger"}>{error}</Alert>}
        {codeDiff && codeDiff.entries.length > 0 && (
            <div className={"table-container"}>
                <table className={"table table-hover"}>
                    <thead>
                    <tr>
                        <th>Name</th>
                        <th>Database</th>
                        <th></th>
                        <th>File</th>
                    </tr>
                    </thead>
                    <tbody>
                    {codeDiff.entries.slice(0, 100).map(codeDiffEntry => {
                        const fqName = tableNameToFqName(codeDiffEntry.tableName);
                        return (
                            <tr key={fqName}>
                                <td>{fqName}</td>
                                <td>
                                    {getFileStatus(codeDiffEntry)}
                                </td>
                                <td>
                                    {getButton(codeDiffEntry)}
                                </td>
                                <td>
                                    {getDatabaseStatus(codeDiffEntry)}
                                </td>
                            </tr>
                        );
                    })}
                    </tbody>
                </table>
            </div>
        )}
        {codeDiff?.entries?.length === 0 && <h5 className={"text-center"}>No Changes Detected</h5>}
    </div>
}