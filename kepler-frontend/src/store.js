import { createStore, combineReducers, applyMiddleware } from "redux";
import keplerGlReducer from "kepler.gl/reducers";
import { taskMiddleware } from "react-palm/tasks";
import { MAP_STYLE_OSM } from "./kepler-setup";

// Customizujemo Kepler reducer sa custom map stilom
const customizedKeplerGlReducer = keplerGlReducer.initialState({
  mapStyle: {
    mapStyles: {
      osm: {
        id: 'osm',
        label: 'CartoDB Positron',
        url: '',
        style: MAP_STYLE_OSM,
        icon: 'https://a.basemaps.cartocdn.com/light_all/0/0/0.png'
      }
    },
    styleType: 'osm'
  }
});

// Kombinujemo Kepler reducer sa drugim reducer-ima (ako ih bude)
const reducers = combineReducers({
  keplerGl: customizedKeplerGlReducer
});

// Kreiramo store sa task middleware-om
const store = createStore(reducers, {}, applyMiddleware(taskMiddleware)); //depricated

export default store;