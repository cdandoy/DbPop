<#-- @ftlvariable name="datasetModels" type="java.util.List<org.dandoy.dbpopd.WelcomeController.DatasetModel>" -->
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
                        <a href="/api-docs/" class="nav-link text-white">
                            <i class="fa-solid fa-code"></i>
                            API
                        </a>
                    </li>
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
    <div class="row">
        <div class="col-3"></div>
        <div class="col-6">
            <div class="list-group mx-0 datasets">
                <label class="list-group-item d-flex gap-2 title">
                    Datasets
                </label>
                <#list datasetModels as datasetModel>
                    <label class="list-group-item d-flex gap-2">
                        ${datasetModel}
                    </label>
                </#list>
            </div>
        </div>
    </div>
</div>
<script src="https://code.jquery.com/jquery-3.6.1.min.js" integrity="sha256-o88AwQnZB+VDvE9tvIXrMQaPlFFSUTR+nldQm1LuPXQ=" crossorigin="anonymous"></script>
<script src="/welcome.js"></script>
</body>
</html>
