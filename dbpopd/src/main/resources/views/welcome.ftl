<#-- @ftlvariable name="datasetNames" type="java.util.List<java.lang.String>" -->
<#-- @ftlvariable name="datasetStatuses" type="java.util.List<org.dandoy.dbpopd.WelcomeController.DatasetStatus>" -->
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

    <ul class="nav nav-tabs" id="myTab" role="tablist">
        <li class="nav-item" role="presentation">
            <button class="nav-link " id="datasets-tab" data-bs-toggle="tab" data-bs-target="#datasets-tab-pane" type="button" role="tab" aria-controls="datasets-tab-pane" aria-selected="true">Datasets</button>
        </li>
        <li class="nav-item" role="presentation">
            <button class="nav-link active" id="status-tab" data-bs-toggle="tab" data-bs-target="#status-tab-pane" type="button" role="tab" aria-controls="status-tab-pane" aria-selected="false">Status</button>
        </li>
        <li class="nav-item" role="presentation">
            <button class="nav-link" id="apidoc-tab" data-bs-toggle="tab" data-bs-target="#apidoc-tab-pane" type="button" role="tab" aria-controls="apidoc-tab-pane" aria-selected="false">API</button>
        </li>
    </ul>
    <div class="tab-content" id="myTabContent">
        <div class="tab-pane fade " id="datasets-tab-pane" role="tabpanel" aria-labelledby="datasets-tab" tabindex="0">
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
        <div class="tab-pane fade show active" id="status-tab-pane" role="tabpanel" aria-labelledby="status-tab" tabindex="0">

            <div class="row mt-3">
                <div class="col-6">
                    <div class="card">
                        <div class="card-body">
                            <h5 class="card-title">Files</h5>
                            <div class="accordion" id="status-accordion">
                                <#list 0..datasetStatuses?size-1 as datasetStatusIndex>
                                    <div class="accordion-item">
                                        <h2 class="accordion-header" id="status-heading-${datasetStatusIndex}">
                                            <button class="accordion-button collapsed" type="button" data-bs-toggle="collapse" data-bs-target="#status-${datasetStatusIndex}" aria-expanded="false" aria-controls="status-${datasetStatusIndex}">
                                                ${datasetStatuses[datasetStatusIndex].name}
                                            </button>
                                        </h2>
                                        <div id="status-${datasetStatusIndex}" class="accordion-collapse collapse" aria-labelledby="status-heading-${datasetStatusIndex}" data-bs-parent="#status-accordion">
                                            <div class="accordion-body">
                                                <table class="table table-hover">
                                                    <thead>
                                                    <tr>
                                                        <th>Table</th>
                                                        <th>Rows</th>
                                                        <th>Size</th>
                                                    </tr>
                                                    </thead>
                                                    <tbody>
                                                    <#list datasetStatuses[datasetStatusIndex].datasetFileStatuses as datasetFileStatus>
                                                        <tr>
                                                            <td>${datasetFileStatus.tableName}</td>
                                                            <td>${datasetFileStatus.rows}</td>
                                                            <td>${datasetFileStatus.size}</td>
                                                        </tr>
                                                    </#list>
                                                    </tbody>
                                                </table>
                                            </div>
                                        </div>
                                    </div>
                                </#list>
                            </div>
                        </div>
                    </div>

                </div>
                <div class="col-6">
                    <div class="card">
                        <div class="card-body">
                            <h5 class="card-title">Tables</h5>
                            <div class="accordion" id="tables-accordion">
                            </div>
                        </div>
                    </div>
                </div>
            </div>
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
