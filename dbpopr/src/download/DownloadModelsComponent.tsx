import React, {useState} from "react";

export default function DownloadModelsComponent() {
    const [models, setModels] = useState<string[]>([]);

    return (
        <div className="card models">
            <div className="card-body">
                <h5 className="card-title">Models</h5>
                <div className="datasets p-3">
                    {models.length == 0 && <div className="text-center">No Models</div>}
                </div>

                <div className="text-end">
                    <a className="card-link">Create Model</a>
                </div>
            </div>
        </div>
    )
}