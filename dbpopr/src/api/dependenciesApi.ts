import axios, {AxiosResponse} from "axios";
import {Dependency} from "../models/Dependency";

export default function dependenciesApi(dependency: Dependency) {
    return axios.post<Dependency, AxiosResponse<Dependency>>(`/database/dependencies`, dependency)
}