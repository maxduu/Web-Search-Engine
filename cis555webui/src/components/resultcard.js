import React, { useState } from 'react';
import Card from 'react-bulma-components/lib/components/card';

const ResultsCard = () => {
  const [info, setInfo] = useState({});

  return (
    <Card>
      <Card.Header>
        <a href={}></a>
      </Card.Header>
      <Card.Content>{}</Card.Content>
    </Card>
  );
};

export default ResultsCard;
