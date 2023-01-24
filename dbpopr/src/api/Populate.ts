import axios from "axios";
import {PopulateResult} from "../models/PopulateResult";

export function populate(datasetName: string) {
    return axios.get<PopulateResult>("/populate", {params: {dataset: datasetName}})
}