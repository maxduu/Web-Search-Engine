import React, { useEffect, useState } from 'react';
import Container from 'react-bulma-components/lib/components/container';
import Pagination from 'react-bulma-components/lib/components/pagination';
import { Field, Input } from 'react-bulma-components/lib/components/form';
import Button from 'react-bulma-components/lib/components/button';
import Card from 'react-bulma-components/lib/components/card';
import Image from 'react-bulma-components/lib/components/image';
import Heading from 'react-bulma-components/lib/components/heading';
import axios from 'axios';

import ResultsCard from '../components/resultcard';
import * as ROUTES from '../constants/routes';
const logo = '../../walmart-google.png';

const ResultsPage = () => {
  const [searchTerm, setSearchTerm] = useState('');
  const [results, setResults] = useState([]);
  const [displayResults, setDisplayResults] = useState([]);
  const [currentPage, setCurrentPage] = useState(0);
  const [totalPages, setTotalPages] = useState(0);

  useEffect(() => {
    const split = window.location.search.substring(1).split('&');
    for (let i = 0; i < split.length; i++) {
      if (split[i].startsWith('search=')) {
        const searchPageSearchTerm = split[i].split('search=')[1];
        setSearchTerm(searchPageSearchTerm);
        search(searchPageSearchTerm);
        break;
      }
    }
  }, []);

  const search = (query) => {
    axios
      .get(`http://${ROUTES.HOSTNAME}:${ROUTES.PORT}/search?query=${query}`)
      .then((res) => {
        console.log(res)
        if (!res || !res.data.length) {
          setResults([]);
          setDisplayResults([]);
          setCurrentPage(0);
          setTotalPages(0);
        } else {
          const resultHTML = res.data.map((result, i) => <ResultsCard key={i} resultData={result} />);
          console.log(resultHTML)
          setResults(resultHTML);
          setDisplayResults(resultHTML.slice(0, 10));
          setCurrentPage(1);
          setTotalPages(Math.ceil(res.length / 10));
        }
      })
      .catch((err) => {
        console.log(err);
      });
  };

  const onChangePage = (e) => {
    setDisplayResults(results.slice((e - 1) * 10, e * 10));
    setCurrentPage(e);
  };

  return (
    <>
      <Card>
        <Card.Content>
          <Field kind='addons'>
            <a href={ROUTES.SEARCH} style={{ width: '25%' }}>
              <Image alt='Walmart-Brand Google' src={logo} />
            </a>
            <div style={{ width: '5%' }} />
            <Input
              placeholder='Search'
              onChange={(e) => setSearchTerm(e.target.value)}
              value={searchTerm}
              style={{ width: '30%' }}
            />
            <Button
              renderAs='button'
              onClick={() => {console.log(searchTerm); search(searchTerm)}}
              disabled={!searchTerm.trim().length}>
              Search
            </Button>
          </Field>
        </Card.Content>
      </Card>
      <br />
      <Container>
        <div>
          <Heading>Results</Heading>
          {displayResults}
        </div>
      </Container>
      <br />
      <Container>
        <Pagination
          current={currentPage}
          total={totalPages}
          onChange={onChangePage}
          style={{ marginBottom: '10px' }}
        />
      </Container>
    </>
  );
};

export default ResultsPage;
