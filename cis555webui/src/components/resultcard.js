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
          {resultData.url.substring(0, Math.min(resultData.url.length, 110)) +
            (resultData.url.length > 110 ? ' ...' : '')}
        </Heading>
        <Heading size={4}>
          <a href={resultData.url}>
            {resultData.title.substring(0, Math.min(resultData.title.length, 70)) +
              (resultData.title.length > 70 ? ' ...' : '')}
          </a>
        </Heading>
        <Heading style={{ fontWeight: 'normal' }} size={6}>
          <i>
            {resultData.preview.substring(0, Math.min(resultData.preview.length, 600)) +
              (resultData.preview.length > 600 ? ' ...' : '')}
          </i>
        </Heading>
      </Card.Content>
    </Card>
    <br />
  </>
);

export default ResultsCard;
