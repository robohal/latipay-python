var API_KEY = '0e3b3e35f5951b0b9c5ade5e34141494';

var gb = require('geckoboard')(
  API_KEY
);

gb.datasets.findOrCreate(
  {
    id: 'sales.by_day',
    fields: {
      quantity: {
        type: 'number',
        name: 'Number of sales'
      },
      gross: {
        type: 'money',
        name: 'Gross value of sales',
        currency_code: "USD"
      },
      date: {
        type: 'date',
        name: 'Date'
      }
    }
  },
  function (err, dataset) {
    if (err) {
      console.error(err);
      return;
    }

    console.log('Dataset created');
  }
);
