import React from 'react';
import Card from 'react-bulma-components/lib/components/card';

const ResultsCard = (resultData) => (
  <Card>
    <Card.Content>
      <a href={resultData.resultData.url}><h3>{resultData.resultData.title}</h3></a>
        <h6>Url: {resultData.resultData.url}</h6>
        <p>{resultData.resultData.score}</p>
    </Card.Content>
  </Card>
);

export default ResultsCard;
