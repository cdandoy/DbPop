import React, {useEffect, useState} from "react";
import PageHeader from "../../../pageheader/PageHeader";
import LoadingOverlay from "../../../utils/LoadingOverlay";
import {CodeDiff, CodeDiffEntry, compareSourceToFile} from "../../../../api/codeApi";
import {Alert} from "react-bootstrap";
import {tableNameToFqName} from "../../../../models/TableName";
import "../CodeCompare.scss"
import compare_source from "./compare_source.png"

export default function CodeSourceCompare() {
    const [loading, setLoading] = useState(false);
    const [codeDiff, setCodeDiff] = useState<CodeDiff | undefined>();
    const [error, setError] = useState<string | undefined>();

    useEffect(() => {
        setLoading(true);
        setError(undefined);
        compareSourceToFile()
            .then(result => setCodeDiff(result.data))
            .catch((error) => setError(error))
            .finally(() => setLoading(false));
    }, []);

    function getDatabaseText(codeDiffEntry: CodeDiffEntry) {
        const databaseTime = codeDiffEntry.databaseTime;
        const fileTime = codeDiffEntry.fileTime;
        if (!databaseTime) return "";
        if (!fileTime) return "New";
        if (databaseTime < fileTime) return "Older";
        if (databaseTime > fileTime) return "Newer";
        return "Same";
    }

    function getFileText(codeDiffEntry: CodeDiffEntry) {
        const databaseTime = codeDiffEntry.databaseTime;
        const fileTime = codeDiffEntry.fileTime;
        if (!fileTime) return "";
        if (!databaseTime) return "New";
        if (fileTime < databaseTime) return "Older";
        if (fileTime > databaseTime) return "Newer";
        return "Same"
    }

    return <div id={"code-source-compare"}>
        <PageHeader title={"Compare Source"} subtitle={"Compare the tables and sprocs in the source database with the local SQL files"} tool={<img src={compare_source} style={{width: "20em"}} alt={"Compare Source"}/>}/>
        <LoadingOverlay active={loading}/>
        {error && <Alert variant={"danger"}>{error}</Alert>}
        {codeDiff && (
            <div className={"table-container"}>
                <table className={"table table-hover"}>
                    <thead>
                    <tr>
                        <th>Name</th>
                        <th>Database</th>
                        <th>File</th>
                        <th></th>
                    </tr>
                    </thead>
                    <tbody>
                    {codeDiff.entries.slice(0, 100).map(codeDiffEntry => {
                        const fqName = tableNameToFqName(codeDiffEntry.tableName);
                        return (
                            <tr key={fqName}>
                                <td>{fqName}</td>
                                <td>
                                    {getDatabaseText(codeDiffEntry)}
                                </td>
                                <td>
                                    {getFileText(codeDiffEntry)}
                                </td>
                            </tr>
                        )
                    })}
                    </tbody>
                </table>
            </div>
        )}
    </div>
}