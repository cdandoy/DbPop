<#-- @ftlvariable name="datasetNames" type="java.util.List<java.lang.String>" -->
<#-- @ftlvariable name="datasetFileRows" type="java.util.List<org.dandoy.dbpopd.WelcomeController.DatasetFileRow>" -->
<#-- @ftlvariable name="sqlSetupStatus" type="org.dandoy.dbpopd.WelcomeController.SqlSetupStatus" -->
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>dbpop</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.2.3/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-rbsA2VBKQhggwzxH7pPCaAqO46MgnOM80zW1RWuH61DGLwZJEdK2Kadq2F9CUG65" crossorigin="anonymous">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.2.1/css/all.min.css" integrity="sha512-MV7K8+y+gLIBoVD59lQIYicR65iaqukzvf/nwasF0nqhPay5w/9lJmVM2hMDcnK1OnMGCdVK+iQrJ7lzPJQd1w==" crossorigin="anonymous" referrerpolicy="no-referrer"/>
    <link rel="stylesheet" href="/welcome.css"/>
</head>
<body>

<header>
    <div class="px-3 py-2 text-bg-dark">
        <div class="container">
            <div class="d-flex flex-wrap align-items-center justify-content-center justify-content-lg-start">
                <a href="/" class="d-flex align-items-center my-2 my-lg-0 me-lg-auto text-white text-decoration-none">
                    <h1>DbPop</h1>
                </a>

                <ul class="nav col-12 col-lg-auto my-2 justify-content-center my-md-0 text-small">
                    <li>
                        <a href="https://github.com/cdandoy/DbPop" class="nav-link text-white">
                            <i class="fa-brands fa-docker"></i>
                            GitHub
                        </a>
                    </li>
                    <li>
                        <a href="https://hub.docker.com/repository/docker/cdandoy/dbpop" class="nav-link text-white">
                            <i class="fa-brands fa-github"></i>
                            Docker
                        </a>
                    </li>
                </ul>
            </div>
        </div>
    </div>
</header>

<div class="container">
    <div class="text-center m-5">
        <h1>Welcome to DbPop</h1>
        <p class="lead">The easiest way to populate your development database.</p>
    </div>

    <#if !sqlSetupStatus.loaded()>
        <div class="mb-4"><i class="fa fa-fw fa-spinner fa-spin"></i> setup.sql loading...</div>
    <#elseif sqlSetupStatus.errorMessage()??>
        <pre class="mb-4 alert alert-danger" role="alert">${sqlSetupStatus.errorMessage()}</pre>
    </#if>

    <ul class="nav nav-tabs" id="myTab" role="tablist">
        <li class="nav-item" role="presentation">
            <button class="nav-link active" id="datasets-tab" data-bs-toggle="tab" data-bs-target="#datasets-tab-pane" type="button" role="tab" aria-controls="datasets-tab-pane" aria-selected="true">Datasets</button>
        </li>
        <li class="nav-item" role="presentation">
            <button class="nav-link" id="files-tab" data-bs-toggle="tab" data-bs-target="#files-tab-pane" type="button" role="tab" aria-controls="files-tab-pane" aria-selected="false">Files</button>
        </li>
        <li class="nav-item" role="presentation">
            <button class="nav-link" id="apidoc-tab" data-bs-toggle="tab" data-bs-target="#apidoc-tab-pane" type="button" role="tab" aria-controls="apidoc-tab-pane" aria-selected="false">API</button>
        </li>
    </ul>
    <div class="tab-content" id="myTabContent">
        <div class="tab-pane fade show active" id="datasets-tab-pane" role="tabpanel" aria-labelledby="datasets-tab" tabindex="0">
            <div class="datasets p-2">
                <#list datasetNames as datasetName>
                    <div class="d-flex p-2">
                        <div data-dataset="${datasetName}">
                            <div class='dataset-button'>
                                <button class="btn btn-xs button-load" title="Load">
                                    <i class="fa fa-fw fa-play"></i>
                                    <i class="fa fa-fw fa-spinner fa-spin"></i>
                                </button>
                                <span>${datasetName}</span>
                            </div>
                            <div class='dataset-result'></div>
                            <div class='dataset-error'></div>
                        </div>
                    </div>
                </#list>
            </div>
        </div>
        <div class="tab-pane fade" id="files-tab-pane" role="tabpanel" aria-labelledby="files-tab" tabindex="0">

            <table class="table table-hover mt-3">
                <thead>
                <tr>
                    <th>Dataset</th>
                    <th>Table</th>
                    <th>Rows</th>
                    <th>Size</th>
                    <#--
                                        <th style="width: 1px"></th>
                    -->
                </tr>
                </thead>
                <tbody>
                <#list datasetFileRows as datasetFileRow>
                    <tr>
                        <td>${datasetFileRow.datasetName()}</td>
                        <td>${datasetFileRow.tableName()}</td>
                        <td>${datasetFileRow.rows()}</td>
                        <td>${datasetFileRow.fileSize()}</td>
                        <#--
                                                <td style="white-space: nowrap">
                                                    <a href="#" class="btn btn-xs">
                                                        <i class="fa fa-download"></i>
                                                    </a>
                                                    <a href="#" class="btn btn-xs">
                                                        <i class="fa fa-eye"></i>
                                                    </a>
                                                </td>
                        -->
                    </tr>
                </#list>
                </tbody>
            </table>
        </div>
        <div class="tab-pane fade" id="apidoc-tab-pane" role="tabpanel" aria-labelledby="apidoc-tab" tabindex="0">
            <iframe src="/api-docs/"></iframe>
        </div>
    </div>

</div>
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.2.3/dist/js/bootstrap.bundle.min.js" integrity="sha384-kenU1KFdBIe4zVF0s0G1M5b4hcpxyD9F7jL+jjXkk+Q2h455rYXK/7HAuoJl+0I4" crossorigin="anonymous"></script>
<script src="https://code.jquery.com/jquery-3.6.1.min.js" integrity="sha256-o88AwQnZB+VDvE9tvIXrMQaPlFFSUTR+nldQm1LuPXQ=" crossorigin="anonymous"></script>
<script src="/welcome.js"></script>
</body>
</html>
