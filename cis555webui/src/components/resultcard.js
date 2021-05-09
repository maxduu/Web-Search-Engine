import React from 'react';
import Card from 'react-bulma-components/lib/components/card';
import Heading from 'react-bulma-components/lib/components/heading';

const ResultsCard = ({ resultData }) => (
  <>
    <Card>
      <Card.Content>
        <Heading
          subtitle
          size={6}
          onClick={() => (window.location = resultData.url)}
          style={{ cursor: 'pointer' }}>
          {resultData.url}
        </Heading>
        <Heading size={4}>
          <a href={resultData.url}>{resultData.title}</a>
        </Heading>
      </Card.Content>
    </Card>
    <br />
  </>
);

export default ResultsCard;
