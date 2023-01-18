import {useEffect, useState} from "react";
import datasetsApi from "../../api/datasetsApi";

export default function useDatasets(): [string[], boolean] {
    const [loading, setLoading] = useState(false);
    const [datasets, setDatasets] = useState<string[]>([])

    useEffect(() => {
        setLoading(true);
        datasetsApi()
            .then(result => {
                setDatasets(result.data);
                setLoading(false);
            })
    }, []);

    return [datasets, loading];
}