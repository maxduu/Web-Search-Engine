import React from 'react';
import Card from 'react-bulma-components/lib/components/card';

const ResultsCard = (resultData) => (
  <Card>
    <Card.Content>
      <a href={resultData.resultData.url}>{resultData.resultData.url}</a>
    </Card.Content>
  </Card>
);

export default ResultsCard;
