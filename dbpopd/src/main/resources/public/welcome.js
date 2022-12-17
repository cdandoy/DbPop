$(function () {
    $('.button-load').click(function (event) {
        let $datasetDiv = $(event.target).closest('[data-dataset]');
        let dataset = $datasetDiv.data('dataset');
        $('.button-load').attr('disabled', 'disabled');
        $('.dataset-result').empty();
        $datasetDiv.find('.dataset-result')
            .append('<div><label>Loading</label></div>');
        $('.dataset-error').empty();
        $datasetDiv.find('.fa-play').hide();
        $datasetDiv.find('.fa-spinner').css('display', 'inherit');
        $.ajax({
            url: "/populate?dataset=" + dataset,
        })
            .done(function (data) {
                $datasetDiv.find('.dataset-result')
                    .empty()
                    .append(`<div><label>Loaded ${data.rows} rows in ${data.millis}ms</label></div>`);
            })
            .fail(function (response) {
                function addError(message) {
                    $datasetDiv.find('.dataset-error').append(`<div><label class="text-danger">${message}</label> </div>`);
                }

                try {
                    $('.dataset-result').empty();
                    if (response.responseJSON._embedded.errors) {
                        for (const error of response.responseJSON._embedded.errors) {
                            if (error.message) {
                                let messages = error.message.split('\n');
                                for (const message of messages) {
                                    addError(message);
                                }
                            }
                        }
                    } else if (response.responseText) {
                        addError(response.responseText);
                    } else {
                        addError("Failed");
                    }
                } catch (e) {
                    console.log(e);
                }
            })
            .always(function () {
                $datasetDiv.find('.fa-play').show();
                $datasetDiv.find('.fa-spinner').hide();
                $('.button-load').attr('disabled', null);
            });
    })
});
