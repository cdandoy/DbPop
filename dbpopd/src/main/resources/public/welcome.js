$(function () {
    $('.button-load').click(function (event) {
        let $datasetDiv = $(event.target).closest('[data-dataset]');
        let dataset = $datasetDiv.data('dataset');
        $('.button-load').attr('disabled', 'disabled');
        $('.dataset-result').empty()
            .append('<div><label>Loading</label></div>');
        $('.dataset-error').empty();
        $datasetDiv.find('.fa-play').hide();
        $datasetDiv.find('.fa-spinner').show();
        $.ajax({
            url: "/populate?dataset=" + dataset,
        })
            .done(function (data) {
                $datasetDiv.find('.dataset-result')
                    .empty()
                    .append(`<div><label>Loaded ${data.rows} rows in ${data.millis}ms</label></div>`);
            })
            .fail(function (response) {
                $('.dataset-result').empty();
                for (const error of response.responseJSON._embedded.errors) {
                    $datasetDiv.find('.dataset-error').append(`<div><label class="text-danger">${error.message}</label> </div>`);
                }
            })
            .always(function () {
                $datasetDiv.find('.fa-play').show();
                $datasetDiv.find('.fa-spinner').hide();
                $('.button-load').attr('disabled', null);
            });
    })
});
